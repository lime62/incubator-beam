#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Unit tests for the DisplayData API."""

from __future__ import absolute_import

from datetime import datetime
import unittest

import hamcrest as hc
from hamcrest.core.base_matcher import BaseMatcher

import apache_beam as beam
from apache_beam.transforms.display import HasDisplayData
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.utils.pipeline_options import PipelineOptions


class DisplayDataItemMatcher(BaseMatcher):
  """ Matcher class for DisplayDataItems in unit tests.
  """
  IGNORED = object()

  def __init__(self, key=IGNORED, value=IGNORED,
               namespace=IGNORED, label=IGNORED, shortValue=IGNORED):
    if all(member == DisplayDataItemMatcher.IGNORED for member in
           [key, value, namespace, label, shortValue]):
      raise ValueError('Must receive at least one item attribute to match')

    self.key = key
    self.value = value
    self.namespace = namespace
    self.label = label
    self.shortValue = shortValue

  def _matches(self, item):
    if self.key != DisplayDataItemMatcher.IGNORED and item.key != self.key:
      return False
    if (self.namespace != DisplayDataItemMatcher.IGNORED and
        item.namespace != self.namespace):
      return False
    if (self.value != DisplayDataItemMatcher.IGNORED and
        item.value != self.value):
      return False
    if (self.label != DisplayDataItemMatcher.IGNORED and
        item.label != self.label):
      return False
    if (self.shortValue != DisplayDataItemMatcher.IGNORED and
        item.shortValue != self.shortValue):
      return False
    return True

  def describe_to(self, description):
    descriptors = []
    if self.key != DisplayDataItemMatcher.IGNORED:
      descriptors.append('key is {}'.format(self.key))
    if self.value != DisplayDataItemMatcher.IGNORED:
      descriptors.append('value is {}'.format(self.value))
    if self.namespace != DisplayDataItemMatcher.IGNORED:
      descriptors.append('namespace is {}'.format(self.namespace))
    if self.label != DisplayDataItemMatcher.IGNORED:
      descriptors.append('label is {}'.format(self.label))
    if self.shortValue != DisplayDataItemMatcher.IGNORED:
      descriptors.append('shortValue is {}'.format(self.shortValue))

    item_description = '{}'.format(' and '.join(descriptors))
    description.append(item_description)


class DisplayDataTest(unittest.TestCase):

  def test_display_data_item_matcher(self):
    with self.assertRaises(ValueError):
      DisplayDataItemMatcher()

  def test_inheritance_ptransform(self):
    class MyTransform(beam.PTransform):
      pass

    display_pt = MyTransform()
    # PTransform inherits from HasDisplayData.
    self.assertTrue(isinstance(display_pt, HasDisplayData))
    self.assertEqual(display_pt.display_data(), {})

  def test_inheritance_dofn(self):
    class MyDoFn(beam.DoFn):
      pass

    display_dofn = MyDoFn()
    self.assertTrue(isinstance(display_dofn, HasDisplayData))
    self.assertEqual(display_dofn.display_data(), {})

  def test_unsupported_type_display_data(self):
    class MyDisplayComponent(HasDisplayData):
      def display_data(self):
        return {'item_key': 'item_value'}

    with self.assertRaises(ValueError):
      DisplayData.create_from_options(MyDisplayComponent())

  def test_create_list_display_data(self):
    flags = ['--extra_package', 'package1', '--extra_package', 'package2']
    pipeline_options = PipelineOptions(flags=flags)
    items = DisplayData.create_from_options(pipeline_options).items
    hc.assert_that(items, hc.contains_inanyorder(
        DisplayDataItemMatcher('extra_packages',
                               str(['package1', 'package2']))))

  def test_base_cases(self):
    """ Tests basic display data cases (key:value, key:dict)
    It does not test subcomponent inclusion
    """
    class MyDoFn(beam.DoFn):
      def __init__(self, my_display_data=None):
        self.my_display_data = my_display_data

      def process(self, element):
        yield element + 1

      def display_data(self):
        return {'static_integer': 120,
                'static_string': 'static me!',
                'complex_url': DisplayDataItem('github.com',
                                               url='http://github.com',
                                               label='The URL'),
                'python_class': HasDisplayData,
                'my_dd': self.my_display_data}

    now = datetime.now()
    fn = MyDoFn(my_display_data=now)
    dd = DisplayData.create_from(fn)
    nspace = '{}.{}'.format(fn.__module__, fn.__class__.__name__)
    expected_items = [
        DisplayDataItemMatcher(key='complex_url',
                               value='github.com',
                               namespace=nspace,
                               label='The URL'),
        DisplayDataItemMatcher(key='my_dd',
                               value=now,
                               namespace=nspace),
        DisplayDataItemMatcher(key='python_class',
                               value=HasDisplayData,
                               namespace=nspace,
                               shortValue='HasDisplayData'),
        DisplayDataItemMatcher(key='static_integer',
                               value=120,
                               namespace=nspace),
        DisplayDataItemMatcher(key='static_string',
                               value='static me!',
                               namespace=nspace)]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_drop_if_none(self):
    class MyDoFn(beam.DoFn):
      def display_data(self):
        return {'some_val': DisplayDataItem('something').drop_if_none(),
                'non_val': DisplayDataItem(None).drop_if_none(),
                'def_val': DisplayDataItem(True).drop_if_default(True),
                'nodef_val': DisplayDataItem(True).drop_if_default(False)}

    dd = DisplayData.create_from(MyDoFn())
    expected_items = [DisplayDataItemMatcher('some_val',
                                             'something'),
                      DisplayDataItemMatcher('nodef_val',
                                             True)]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_subcomponent(self):
    class SpecialDoFn(beam.DoFn):
      def display_data(self):
        return {'dofn_value': 42}

    dofn = SpecialDoFn()
    pardo = beam.ParDo(dofn)
    dd = DisplayData.create_from(pardo)
    dofn_nspace = '{}.{}'.format(dofn.__module__, dofn.__class__.__name__)
    pardo_nspace = '{}.{}'.format(pardo.__module__, pardo.__class__.__name__)
    expected_items = [
        DisplayDataItemMatcher('dofn_value', 42, dofn_nspace),
        DisplayDataItemMatcher('fn', SpecialDoFn, pardo_nspace)]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))


# TODO: Test __repr__ function
# TODO: Test PATH when added by swegner@
if __name__ == '__main__':
  unittest.main()
