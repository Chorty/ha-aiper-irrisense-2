"""Synthetic device-report field extraction and status-query cadence; no device or HA needed.

The pure helpers are loaded directly. The coordinator's actual status-query and MQTT-intake methods are compiled
from source as in test_mqtt. All payloads are synthetic; sockets/DNS/processes are blocked.
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
from types import SimpleNamespace
import unittest
from unittest.mock import patch

COMPONENT = Path(__file__).parents[1] / 'custom_components' / 'aiper_irrisense'


def load_file(name):
    spec = importlib.util.spec_from_file_location('device_report_test_' + name, COMPONENT / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


report = load_file('device_report')
constants = load_file('const')
protocol = load_file('mqtt')
schedule = load_file('schedule')


def frame(cmd, body, ts):
    return {'type': cmd, 'data': body, '_ts': ts}


class FieldTests(unittest.TestCase):
    def test_freshest_status_frame_wins(self):
        slot = {'up_workInfo': frame('workInfo', {'status': 0, 'valve': 53, 'rotate': 0, 'waterpress': 102.7}, 10.0),
                'up_realtimeStatusReport': frame('realtimeStatusReport', {'status': 3, 'valve': 2000, 'waterpress': 107.8}, 20.0)}
        self.assertEqual(report.latest_field(slot, 'valve'), (2000, 20.0))
        self.assertEqual(report.latest_field(slot, 'waterpress'), (107.8, 20.0))
        self.assertEqual(report.latest_field(slot, 'rotate'), (0, 10.0))      # only workInfo carries it
        self.assertEqual(report.latest_field(slot, 'hydropenia'), (None, None))

    def test_frames_without_usable_timestamp_or_body_are_ignored(self):
        slot = {'up_workInfo': {'type': 'workInfo', 'data': {'valve': 5}},             # no _ts
                'up_workInfoReport': frame('workInfoReport', ['valve'], 30.0),          # non-dict body
                'up_realtimeStatus': frame('realtimeStatus', {'valve': 7}, True),      # bool is not a time
                'up_setWorkMode': frame('setWorkMode', {'valve': 9}, 40.0)}            # not a status source
        self.assertEqual(report.latest_field(slot, 'valve'), (None, None))

    def test_heading_is_signed_degrees(self):
        self.assertEqual(report.heading_degrees(32990), -30.1)
        self.assertEqual(report.heading_degrees(3009), 30.09)
        self.assertEqual(report.heading_degrees(0), 0.0)
        self.assertEqual(report.heading_degrees(18000), 180.0)
        self.assertIsNone(report.heading_degrees('x'))
        self.assertIsNone(report.heading_degrees(True))

    def test_flag_values(self):
        for value, expected in ((1, True), (0, False), (True, True), ('1', True), ('0', False), (None, None), ('?', None)):
            with self.subTest(value=value):
                self.assertIs(report.as_flag(value), expected)


class SectionTests(unittest.TestCase):
    def test_sections_found_in_shadow_documents_but_not_previous_or_metadata(self):
        doc = {'previous': {'state': {'reported': {'NetStat': {'sta': 'old'}}}},
               'current': {'state': {'reported': {'NetStat': {'sta': 1, 'ble': 0}, 'OpInfo': {'status': 2}}},
                           'metadata': {'reported': {'AlarmReport': {'code': 'meta'}}}}}
        self.assertEqual(report.extract_sections(doc), {'net_stat': {'sta': 1, 'ble': 0}, 'op_info': {'status': 2}})

    def test_alarm_under_either_name_and_command_keyed_frames(self):
        self.assertEqual(report.extract_sections({'state': {'reported': {'AlarmReport': {'code': 7}}}}),
                         {'alarm': {'code': 7}})
        self.assertEqual(report.extract_sections({'Alarm': {'code': 3, 'timestamp': 1700000000}, 'res': 0}),
                         {'alarm': {'code': 3, 'timestamp': 1700000000}})
        self.assertEqual(report.extract_sections({'type': 'up_Alarm', 'data': {'code': 4}}), {'alarm': {'code': 4}})
        self.assertEqual(report.extract_sections({'NetStat': 'not a dict'}), {})

    def test_raw_snapshot_lists_frames_newest_time_and_drops_duplicates(self):
        slot = {'up_workInfo': {**frame('workInfo', {'valve': 53}, 10.0), '_raw': {'workInfo': {'valve': 53}}, '_sn': 'S'},
                'shadow_get': {'state': {'reported': {}}, '_ts': 25.0, '_topic': 't'},
                'other': [{'topic': 'x'}],
                'sections': {'alarm': {'data': {'code': 1}, '_ts': 5.0}}}
        newest, frames = report.raw_snapshot(slot)
        self.assertEqual(newest, 25.0)
        self.assertEqual(sorted(frames), ['shadow_get', 'up_workInfo'])
        self.assertNotIn('_raw', frames['up_workInfo'])
        self.assertNotIn('_sn', frames['up_workInfo'])
        self.assertEqual(report.raw_snapshot({}), (None, {}))


def coordinator_type(clock):
    tree = ast.parse((COMPONENT / 'coordinator.py').read_text())
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'IrrisenseCoordinator')
    names = {'_maybe_query_work_info', 'handle_mqtt_message', '_async_handle_mqtt_message', 'trigger_fast_poll'}
    cls.bases = []
    cls.body = [n for n in cls.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
    helpers = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_extract_map_id'
               or isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id.startswith('_STATUS_')
                                                    for t in n.targets)]
    namespace = dict(vars(constants))
    namespace.update(callback=lambda f: f, deepcopy=deepcopy, time=SimpleNamespace(time=lambda: clock[0]),
                     _LOGGER=logging.getLogger(__name__), parse_overview=schedule.parse_overview,
                     iter_upchan_commands=protocol.iter_upchan_commands, latest_field=report.latest_field,
                     extract_sections=report.extract_sections)
    module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0),
                              *helpers, cls], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(COMPONENT / 'coordinator.py'), 'exec'), namespace)
    return namespace['IrrisenseCoordinator']


class StatusQueryTests(unittest.TestCase):
    def setUp(self):
        self.clock = [1000.0]
        self.sent = []
        self.connected = True
        coordinator = coordinator_type(self.clock).__new__(coordinator_type(self.clock))
        coordinator._data = {}
        coordinator._status_query_s = 300
        coordinator._last_status_query = {}
        coordinator.api = SimpleNamespace(is_mqtt_connected=lambda: self.connected,
                                          query_work_info=lambda sn: self.sent.append(sn) or True,
                                          note_upchan_ack=lambda sn, cmd: None)

        async def executor(fn, *args):
            return fn(*args)
        coordinator.hass = SimpleNamespace(async_add_executor_job=executor,
                                           loop=SimpleNamespace(call_soon_threadsafe=lambda fn, *a: fn(*a)))
        coordinator._fast_until = 0.0
        coordinator._fast_interval = 5
        coordinator.fast_polls = 0
        coordinator.trigger_fast_poll = lambda *a: setattr(coordinator, 'fast_polls', coordinator.fast_polls + 1)
        coordinator.async_set_updated_data = lambda data: None
        self.coordinator = coordinator

    def poll(self):
        asyncio.run(self.coordinator._maybe_query_work_info('SN'))

    def test_queries_every_five_minutes_while_idle(self):
        self.poll()
        self.clock[0] += 120
        self.poll()                                   # base poll 2 min later: too soon
        self.clock[0] += 181
        self.poll()
        self.assertEqual(self.sent, ['SN', 'SN'])

    def test_disabled_disconnected_or_failed_send(self):
        self.coordinator._status_query_s = 0
        self.poll()
        self.coordinator._status_query_s = 300
        self.connected = False
        self.poll()
        self.assertEqual(self.sent, [])
        self.connected = True
        self.coordinator.api.query_work_info = lambda sn: False
        self.poll()
        self.assertEqual(self.coordinator._last_status_query, {})   # a failed send retries next poll

    def test_skips_while_device_recently_reported_busy(self):
        self.coordinator._data = {'SN': {'mqtt': {'up_realTimeProgress': frame('realTimeProgress', {'status': 1}, 950.0)}}}
        self.poll()
        self.assertEqual(self.sent, [])
        self.clock[0] = 1100.0                        # busy report is now >120 s old
        self.poll()
        self.assertEqual(self.sent, ['SN'])

    def test_intake_stamps_buckets_and_keeps_sections(self):
        self.coordinator.handle_mqtt_message('SN', {'_topic': '$aws/things/SN/shadow/get/accepted',
                                                    'state': {'reported': {'NetStat': {'sta': 1}}}})
        self.clock[0] += 10
        self.coordinator.handle_mqtt_message('SN', {'_topic': 'aiper/things/SN/WR/cloud/report', 'beat': 1})
        mqtt = self.coordinator._data['SN']['mqtt']
        self.assertEqual(mqtt['shadow_get']['_ts'], 1000.0)
        self.assertEqual(mqtt['cloud_report']['_ts'], 1010.0)
        self.assertEqual(mqtt['sections']['net_stat'], {'data': {'sta': 1}, '_ts': 1000.0})

    def test_idle_reply_to_own_query_opens_no_fast_poll_window(self):
        reply = {'_topic': 'aiper/things/SN/upChan', 'workInfo': {'status': 0, 'valve': 53, 'waterpress': 102.7}}
        self.poll()                                               # our query at t=1000
        self.clock[0] += 2
        self.coordinator.handle_mqtt_message('SN', reply)
        self.assertEqual(self.coordinator.fast_polls, 0)
        self.assertEqual(self.coordinator._data['SN']['mqtt']['up_workInfo']['data']['valve'], 53)   # still stored
        self.clock[0] += 60                                       # unrelated later idle report: unchanged behaviour
        self.coordinator.handle_mqtt_message('SN', reply)
        self.assertEqual(self.coordinator.fast_polls, 1)
        self.coordinator.handle_mqtt_message('SN', {'_topic': 'aiper/things/SN/upChan', 'workInfo': {'status': 1}})
        self.assertEqual(self.coordinator.fast_polls, 2)          # busy replies still open the window


if __name__ == '__main__':
    with (patch.object(socket.socket, 'connect', side_effect=AssertionError('Network')),
          patch.object(socket, 'getaddrinfo', side_effect=AssertionError('DNS')),
          patch.object(subprocess, 'Popen', side_effect=AssertionError('Process'))):
        unittest.main(verbosity=2)
