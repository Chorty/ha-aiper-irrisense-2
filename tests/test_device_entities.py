"""The new device-report entities against synthetic cached frames, with minimal Home Assistant stubs.

Loads the actual sensor.py / binary_sensor.py with stub HA modules and a stub coordinator/entity base, so entity
properties run unmodified. All payloads are synthetic; sockets/DNS/processes are blocked.
"""
from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

COMPONENT = Path(__file__).parents[1] / 'custom_components' / 'aiper_irrisense'
PACKAGE = 'aiper_irrisense_entity_test'


def stub_modules(slot_ref):
    mods = {}

    def mod(name, **attrs):
        m = ModuleType(name)
        m.__dict__.update(attrs)
        mods[name] = m
        return m

    class Entity:
        pass

    class Names:
        """Any member name is its own value (stands in for HA's enums)."""
        def __getattr__(self, name):
            return name

    for name in ('homeassistant', 'homeassistant.components', 'homeassistant.helpers'):
        mod(name).__path__ = []
    mod('homeassistant.components.sensor', SensorEntity=Entity, SensorDeviceClass=Names(), SensorStateClass=Names())
    mod('homeassistant.components.binary_sensor', BinarySensorEntity=Entity, BinarySensorDeviceClass=Names())
    mod('homeassistant.config_entries', ConfigEntry=object)
    mod('homeassistant.const', EntityCategory=Names(), UnitOfLength=Names(), UnitOfVolume=Names(), UnitOfTime=Names())
    mod('homeassistant.core', HomeAssistant=object)
    mod('homeassistant.helpers.entity_platform', AddEntitiesCallback=object)

    package = mod(PACKAGE)
    package.__path__ = [str(COMPONENT)]

    class IrrisenseEntity(Entity):
        def __init__(self, coordinator, sn, key):
            self.coordinator, self._sn, self._attr_unique_id = coordinator, sn, f'{sn}_{key}'

        @property
        def _mqtt(self):
            return slot_ref['mqtt']
    mod(PACKAGE + '.coordinator', IrrisenseCoordinator=object)
    mod(PACKAGE + '.entity', IrrisenseEntity=IrrisenseEntity)
    mod(PACKAGE + '.geometry', spray_reach_m=lambda x, y: None)
    return mods


def load(name):
    spec = importlib.util.spec_from_file_location(f'{PACKAGE}.{name}', COMPONENT / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ADDRESS = ':'.join(['01', '23', '45', '67', '89', 'AB'])     # synthetic, built at runtime


def frame(cmd, body, ts):
    return {'type': cmd, 'data': body, '_ts': ts}


class EntityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.slot = {'mqtt': {}}
        cls.patcher = patch.dict(sys.modules, stub_modules(cls.slot))
        cls.patcher.start()
        for name in ('const', 'device_report', 'schedule'):
            load(name)
        cls.sensor, cls.binary = load('sensor'), load('binary_sensor')

    @classmethod
    def tearDownClass(cls):
        cls.patcher.stop()

    def setUp(self):
        self.slot['mqtt'] = {
            'up_workInfo': frame('workInfo', {'status': 0, 'valve': 55, 'rotate': 32990, 'waterpress': 102.75}, 1_700_000_000.0),
            'up_realTimeProgress': frame('realTimeProgress', {'status': 1, 'hydropenia': 1}, 1_700_000_100.0),
            'shadow_get': {'state': {'reported': {}}, '_ts': 1_700_000_050.0},
            'sections': {'net_stat': {'data': {'ap': 0, 'ble': 1, 'cert': True, 'nearFieldBind': 0, 'sta': 1}, '_ts': 1.0},
                         'op_info': {'data': {'status': 'ok'}, '_ts': 1.0},
                         'alarm': {'data': {'code': 12, 'timestamp': 1699999999}, '_ts': 1_700_000_200.0}}}

    def make(self, cls, *args):
        return cls(SimpleNamespace(), 'SN', *args)

    def test_status_field_sensors(self):
        s = self.sensor
        self.assertEqual(self.make(s.WaterPressureSensor).native_value, 102.75)
        self.assertEqual(self.make(s.InternalValveSensor).native_value, 55)
        self.assertEqual(self.make(s.HeadHeadingSensor).native_value, -30.1)
        self.assertEqual(self.make(s.DeviceStatusSensor).native_value, 1)          # freshest status frame
        self.assertEqual(self.make(s.InternalValveSensor).extra_state_attributes,
                         {'reported_at': datetime.fromtimestamp(1_700_000_000.0, timezone.utc).isoformat()})
        self.assertIs(self.make(self.binary.WaterShortageBinarySensor).is_on, True)

    def test_sections_alarm_and_raw_report(self):
        s = self.sensor
        values = {key: self.make(s.SectionFieldSensor, section, field, key).native_value
                  for section, field, key in s.SECTION_FIELDS}
        self.assertEqual(values, {'net_ap': 0, 'net_ble': 1, 'net_cert': 1, 'net_near_field_bind': 0, 'net_sta': 1,
                                  'op_status': 'ok'})
        alarm = self.make(s.LastAlarmSensor)
        self.assertEqual(alarm.native_value, 12)
        self.assertEqual(alarm.extra_state_attributes['timestamp'], 1699999999)
        raw = self.make(s.RawReportSensor)
        self.assertEqual(raw.native_value, datetime.fromtimestamp(1_700_000_100.0, timezone.utc))
        self.assertEqual(sorted(raw.extra_state_attributes['frames']), ['shadow_get', 'up_realTimeProgress', 'up_workInfo'])
        self.assertEqual(s.RawReportSensor._unrecorded_attributes, frozenset({'frames'}))

    def test_empty_cache_reads_unknown(self):
        self.slot['mqtt'] = {}
        s = self.sensor
        for cls in (s.WaterPressureSensor, s.InternalValveSensor, s.HeadHeadingSensor, s.DeviceStatusSensor,
                    s.LastAlarmSensor, s.RawReportSensor):
            with self.subTest(cls=cls.__name__):
                self.assertIsNone(self.make(cls).native_value)
        self.assertIsNone(self.make(self.binary.WaterShortageBinarySensor).is_on)

    def test_bluetooth_sensors_read_last_advertisement_without_connecting(self):
        s = self.sensor
        info = SimpleNamespace(rssi=-56, source='proxy', time=100.0)
        bluetooth = ModuleType('homeassistant.components.bluetooth')
        calls = []
        bluetooth.async_last_service_info = lambda hass, address, connectable: calls.append((address, connectable)) or info
        hass = SimpleNamespace(config=SimpleNamespace(components={'bluetooth'}))
        rssi = self.make(s.BluetoothRssiSensor, ADDRESS)
        heard = self.make(s.BluetoothLastHeardSensor, ADDRESS)
        for entity in (rssi, heard):
            entity.hass = hass
        self.assertTrue(rssi.should_poll)
        with patch.dict(sys.modules, {'homeassistant.components.bluetooth': bluetooth}), \
                patch.object(s.time, 'monotonic', return_value=130.0):
            import asyncio
            asyncio.run(rssi.async_update())
            asyncio.run(heard.async_update())
            self.assertEqual(rssi.native_value, -56)
            age = datetime.now(timezone.utc) - heard.native_value
            self.assertAlmostEqual(age.total_seconds(), 30.0, delta=2)
        self.assertEqual(calls, [(ADDRESS, False)] * 2)
        hass.config.components = set()                           # Bluetooth not loaded: unknown, no import
        asyncio.run(rssi.async_update())
        self.assertIsNone(rssi.native_value)


if __name__ == '__main__':
    unittest.main(verbosity=2)
