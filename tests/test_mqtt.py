"""Synthetic app-envelope and coordinator regressions; no device or HA needed.

Like the repository's pure-module tests, file-load helpers without package
setup. Compile selected *actual* coordinator methods to test the MQTT boundary
and active-state consumers without mocking their implementations or installing
Home Assistant. All payloads are synthetic; sockets/DNS/processes are blocked.
"""
from __future__ import annotations

import ast
import asyncio
from copy import deepcopy
import importlib.util
import logging
from pathlib import Path
import socket
import subprocess
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

COMPONENT = Path(__file__).parents[1] / 'custom_components' / 'aiper_irrisense'


def load_file(name):
    spec = importlib.util.spec_from_file_location('mqtt_test_' + name, COMPONENT / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


protocol = load_file('mqtt')
constants = load_file('const')
schedule = load_file('schedule')


def coordinator_type():
    tree = ast.parse((COMPONENT / 'coordinator.py').read_text())
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'IrrisenseCoordinator')
    names = {'handle_mqtt_message', '_async_handle_mqtt_message', 'trigger_fast_poll',
             'active_zone_state', '_region_for', 'zones_for', 'zone_name', 'get_device_data'}
    cls.bases = []
    cls.body = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in names
                or isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == '_ACTIVE_SOURCES' for t in n.targets)]
    helpers = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_extract_map_id']
    namespace = dict(vars(constants))
    namespace.update(callback=lambda f: f, deepcopy=deepcopy, time=SimpleNamespace(time=lambda: 100.0),
                     _LOGGER=logging.getLogger(__name__), parse_overview=schedule.parse_overview,
                     iter_upchan_commands=protocol.iter_upchan_commands)
    module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), *helpers, cls], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(COMPONENT / 'coordinator.py'), 'exec'), namespace)
    return namespace['IrrisenseCoordinator'], namespace


class EnvelopeTests(unittest.TestCase):
    def test_metadata_before_command_is_not_a_command(self):
        frame = {'res': 0, 'chksum': 123, 'realTimeProgress': {'status': 1}, '_sn': 'SYNTHETIC'}
        self.assertEqual(list(protocol.iter_upchan_commands(frame)), [('realTimeProgress', {'status': 1})])

    def test_wrapped_legacy_prefix(self):
        self.assertEqual(list(protocol.iter_upchan_commands({'type': 'up_workInfo', 'data': {'status': 2}})),
                         [('workInfo', {'status': 2})])

    def test_all_object_commands_are_preserved(self):
        self.assertEqual(list(protocol.iter_upchan_commands({'workInfo': {}, 'up_DevInfo': {'version': 'SYNTH'}})),
                         [('workInfo', {}), ('DevInfo', {'version': 'SYNTH'})])

    def test_metadata_only_and_malformed_bodies_are_ignored(self):
        for frame in [{'res': 0, 'chksum': {}}, {'setWorkMode': 0},
                      {'type': 'setWorkMode', 'data': []}, {'workInfo': None}]:
            with self.subTest(frame=frame):
                self.assertEqual(list(protocol.iter_upchan_commands(frame)), [])

    def test_wrapped_envelope_takes_precedence(self):
        self.assertEqual(list(protocol.iter_upchan_commands({'type': 'workInfo', 'data': {}, 'DevInfo': {}})),
                         [('workInfo', {})])


class CoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        cls, self.namespace = coordinator_type()
        self.clock = 100.0
        self.namespace['time'] = SimpleNamespace(time=lambda: self.clock)
        self.coordinator = cls()
        c = self.coordinator
        c.hass = SimpleNamespace(loop=self.loop)
        c.api = SimpleNamespace(note_upchan_ack=Mock())
        c._data = {}
        c._fast_until = 0.0
        c._fast_interval = 5
        c.update_interval = 60
        for name in ['_run_start_ts', '_run_start_zone', '_run_duration', '_run_duration_pct', '_run_last_progress']:
            setattr(c, name, {})
        c._cancel_run_watchdog = Mock()
        self.update_threads = []
        c.async_set_updated_data = lambda data: self.update_threads.append(threading.get_ident())
        self.guards = [patch.object(socket, 'socket', side_effect=AssertionError('network forbidden')),
                       patch.object(socket, 'getaddrinfo', side_effect=AssertionError('DNS forbidden')),
                       patch.object(subprocess, 'Popen', side_effect=AssertionError('process forbidden'))]
        for guard in self.guards:
            guard.start()

    def tearDown(self):
        for guard in reversed(self.guards):
            guard.stop()
        self.loop.close()

    def deliver(self, frame):
        frame = {**frame, '_topic': 'aiper/things/SYNTHETIC/upChan'}
        worker = threading.Thread(target=self.coordinator.handle_mqtt_message, args=('SYNTHETIC', frame))
        worker.start()
        worker.join()
        self.loop.run_until_complete(asyncio.sleep(0))

    def test_worker_does_not_mutate_state_until_loop_runs(self):
        frame = {'workInfo': {'status': 1}, '_topic': 'aiper/things/SYNTHETIC/upChan'}
        worker = threading.Thread(target=self.coordinator.handle_mqtt_message, args=('SYNTHETIC', frame))
        worker.start()
        worker.join()
        self.assertEqual(self.coordinator._data, {})
        self.assertEqual(self.update_threads, [])
        frame['workInfo']['status'] = 0  # caller mutation must not alter queued message
        self.loop.run_until_complete(asyncio.sleep(0))
        self.assertEqual(self.coordinator._data['SYNTHETIC']['mqtt']['up_workInfo']['data']['status'], 1)
        self.assertEqual(self.update_threads, [threading.get_ident()])

    def test_metadata_first_reaches_real_command_and_ack(self):
        self.deliver({'res': 0, 'chksum': 123, 'setWorkMode': {'status': 1, 'map_id': 1}})
        mqtt = self.coordinator._data['SYNTHETIC']['mqtt']
        self.assertIn('up_setWorkMode', mqtt)
        self.assertNotIn('up_res', mqtt)
        self.coordinator.api.note_upchan_ack.assert_called_once_with('SYNTHETIC', 'setWorkMode')

    def test_progress_report_is_consumed_by_active_zone(self):
        self.deliver({'realTimeProgressReport': {'status': 1, 'map_info': {'id': 1}, 'time': 60, 'progress': 10}})
        state = self.coordinator.active_zone_state('SYNTHETIC')
        self.assertEqual(state['zone_id'], 1)
        self.assertEqual(state['time_sec'], 60)
        self.assertEqual(self.coordinator.update_interval, 5)

    def test_report_stop_overrides_older_running_frame_and_acknowledges_stop(self):
        self.deliver({'realTimeProgress': {'status': 1, 'map_info': {'id': 1}, 'time': 60, 'progress': 10}})
        self.assertIsNotNone(self.coordinator.active_zone_state('SYNTHETIC'))
        self.clock += 1
        self.deliver({'realTimeProgressReport': {'status': 0}})
        self.assertIsNone(self.coordinator.active_zone_state('SYNTHETIC'))
        self.coordinator.api.note_upchan_ack.assert_any_call('SYNTHETIC', 'setWorkMode')
        self.coordinator._cancel_run_watchdog.assert_called_once_with('SYNTHETIC')

    def test_report_without_report_suffix_remains_supported(self):
        self.deliver({'type': 'up_realTimeProgress', 'data': {'status': 2, 'map_info': {'id': 2}}})
        self.assertEqual(self.coordinator.active_zone_state('SYNTHETIC')['zone_id'], 2)

    def test_scalar_command_cannot_clear_ack_watchdog(self):
        self.deliver({'setWorkMode': False})
        self.coordinator.api.note_upchan_ack.assert_not_called()
        self.assertEqual(self.coordinator.update_interval, 60)
        self.assertIn('up_raw', self.coordinator._data['SYNTHETIC']['mqtt'])

    def test_schedule_messages_still_accumulate(self):
        self.deliver({'WrPlanDetail': {'plan_id': 1, 'name': 'Synthetic plan'}})
        self.deliver({'WrPlanDetail': {'plan_id': 2, 'name': 'Other synthetic plan'}})
        self.assertEqual(set(self.coordinator._data['SYNTHETIC']['plans']), {1, 2})


if __name__ == '__main__':
    unittest.main(verbosity=2)
