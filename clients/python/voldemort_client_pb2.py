# Generated by the protocol buffer compiler.  DO NOT EDIT!

from google.protobuf import descriptor
from google.protobuf import message
from google.protobuf import reflection
from google.protobuf import service
from google.protobuf import service_reflection
from google.protobuf import descriptor_pb2
_REQUESTTYPE = descriptor.EnumDescriptor(
  name='RequestType',
  full_name='voldemort.RequestType',
  filename='RequestType',
  values=[
    descriptor.EnumValueDescriptor(
      name='GET', index=0, number=0,
      options=None,
      type=None),
    descriptor.EnumValueDescriptor(
      name='GET_ALL', index=1, number=1,
      options=None,
      type=None),
    descriptor.EnumValueDescriptor(
      name='PUT', index=2, number=2,
      options=None,
      type=None),
    descriptor.EnumValueDescriptor(
      name='DELETE', index=3, number=3,
      options=None,
      type=None),
    descriptor.EnumValueDescriptor(
      name='GET_VERSION', index=4, number=4,
      options=None,
      type=None),
  ],
  options=None,
)


GET = 0
GET_ALL = 1
PUT = 2
DELETE = 3
GET_VERSION = 4



_CLOCKENTRY = descriptor.Descriptor(
  name='ClockEntry',
  full_name='voldemort.ClockEntry',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='node_id', full_name='voldemort.ClockEntry.node_id', index=0,
      number=1, type=5, cpp_type=1, label=2,
      default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='version', full_name='voldemort.ClockEntry.version', index=1,
      number=2, type=3, cpp_type=2, label=2,
      default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_VECTORCLOCK = descriptor.Descriptor(
  name='VectorClock',
  full_name='voldemort.VectorClock',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='entries', full_name='voldemort.VectorClock.entries', index=0,
      number=1, type=11, cpp_type=10, label=3,
      default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='timestamp', full_name='voldemort.VectorClock.timestamp', index=1,
      number=2, type=3, cpp_type=2, label=1,
      default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_VERSIONED = descriptor.Descriptor(
  name='Versioned',
  full_name='voldemort.Versioned',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='value', full_name='voldemort.Versioned.value', index=0,
      number=1, type=12, cpp_type=9, label=2,
      default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='version', full_name='voldemort.Versioned.version', index=1,
      number=2, type=11, cpp_type=10, label=2,
      default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_ERROR = descriptor.Descriptor(
  name='Error',
  full_name='voldemort.Error',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='error_code', full_name='voldemort.Error.error_code', index=0,
      number=1, type=5, cpp_type=1, label=2,
      default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='error_message', full_name='voldemort.Error.error_message', index=1,
      number=2, type=9, cpp_type=9, label=2,
      default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='version', full_name='voldemort.Error.version', index=2,
      number=3, type=11, cpp_type=10, label=1,
      default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='available', full_name='voldemort.Error.available', index=3,
      number=4, type=5, cpp_type=1, label=1,
      default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='required', full_name='voldemort.Error.required', index=4,
      number=5, type=5, cpp_type=1, label=1,
      default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='successes', full_name='voldemort.Error.successes', index=5,
      number=6, type=5, cpp_type=1, label=1,
      default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_KEYEDVERSIONS = descriptor.Descriptor(
  name='KeyedVersions',
  full_name='voldemort.KeyedVersions',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='key', full_name='voldemort.KeyedVersions.key', index=0,
      number=1, type=12, cpp_type=9, label=2,
      default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='versions', full_name='voldemort.KeyedVersions.versions', index=1,
      number=2, type=11, cpp_type=10, label=3,
      default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_GETREQUEST = descriptor.Descriptor(
  name='GetRequest',
  full_name='voldemort.GetRequest',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='key', full_name='voldemort.GetRequest.key', index=0,
      number=1, type=12, cpp_type=9, label=1,
      default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_GETRESPONSE = descriptor.Descriptor(
  name='GetResponse',
  full_name='voldemort.GetResponse',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='versioned', full_name='voldemort.GetResponse.versioned', index=0,
      number=1, type=11, cpp_type=10, label=3,
      default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='error', full_name='voldemort.GetResponse.error', index=1,
      number=2, type=11, cpp_type=10, label=1,
      default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_GETVERSIONRESPONSE = descriptor.Descriptor(
  name='GetVersionResponse',
  full_name='voldemort.GetVersionResponse',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='versions', full_name='voldemort.GetVersionResponse.versions', index=0,
      number=1, type=11, cpp_type=10, label=3,
      default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='error', full_name='voldemort.GetVersionResponse.error', index=1,
      number=2, type=11, cpp_type=10, label=1,
      default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_GETALLREQUEST = descriptor.Descriptor(
  name='GetAllRequest',
  full_name='voldemort.GetAllRequest',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='keys', full_name='voldemort.GetAllRequest.keys', index=0,
      number=1, type=12, cpp_type=9, label=3,
      default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_GETALLRESPONSE = descriptor.Descriptor(
  name='GetAllResponse',
  full_name='voldemort.GetAllResponse',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='values', full_name='voldemort.GetAllResponse.values', index=0,
      number=1, type=11, cpp_type=10, label=3,
      default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='error', full_name='voldemort.GetAllResponse.error', index=1,
      number=2, type=11, cpp_type=10, label=1,
      default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_PUTREQUEST = descriptor.Descriptor(
  name='PutRequest',
  full_name='voldemort.PutRequest',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='key', full_name='voldemort.PutRequest.key', index=0,
      number=1, type=12, cpp_type=9, label=2,
      default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='versioned', full_name='voldemort.PutRequest.versioned', index=1,
      number=2, type=11, cpp_type=10, label=2,
      default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_PUTRESPONSE = descriptor.Descriptor(
  name='PutResponse',
  full_name='voldemort.PutResponse',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='error', full_name='voldemort.PutResponse.error', index=0,
      number=1, type=11, cpp_type=10, label=1,
      default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='version', full_name='voldemort.PutResponse.version', index=1,
      number=2, type=11, cpp_type=10, label=1,
      default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_DELETEREQUEST = descriptor.Descriptor(
  name='DeleteRequest',
  full_name='voldemort.DeleteRequest',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='key', full_name='voldemort.DeleteRequest.key', index=0,
      number=1, type=12, cpp_type=9, label=2,
      default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='version', full_name='voldemort.DeleteRequest.version', index=1,
      number=2, type=11, cpp_type=10, label=2,
      default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_DELETERESPONSE = descriptor.Descriptor(
  name='DeleteResponse',
  full_name='voldemort.DeleteResponse',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='success', full_name='voldemort.DeleteResponse.success', index=0,
      number=1, type=8, cpp_type=7, label=2,
      default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='error', full_name='voldemort.DeleteResponse.error', index=1,
      number=2, type=11, cpp_type=10, label=1,
      default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_VOLDEMORTREQUEST = descriptor.Descriptor(
  name='VoldemortRequest',
  full_name='voldemort.VoldemortRequest',
  filename='voldemort-client.proto',
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='type', full_name='voldemort.VoldemortRequest.type', index=0,
      number=1, type=14, cpp_type=8, label=2,
      default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='should_route', full_name='voldemort.VoldemortRequest.should_route', index=1,
      number=2, type=8, cpp_type=7, label=2,
      default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='store', full_name='voldemort.VoldemortRequest.store', index=2,
      number=3, type=9, cpp_type=9, label=2,
      default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='get', full_name='voldemort.VoldemortRequest.get', index=3,
      number=4, type=11, cpp_type=10, label=1,
      default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='getAll', full_name='voldemort.VoldemortRequest.getAll', index=4,
      number=5, type=11, cpp_type=10, label=1,
      default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='put', full_name='voldemort.VoldemortRequest.put', index=5,
      number=6, type=11, cpp_type=10, label=1,
      default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='delete', full_name='voldemort.VoldemortRequest.delete', index=6,
      number=7, type=11, cpp_type=10, label=1,
      default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='requestRouteType', full_name='voldemort.VoldemortRequest.requestRouteType', index=7,
      number=8, type=5, cpp_type=1, label=1,
      default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],  # TODO(robinson): Implement.
  enum_types=[
  ],
  options=None)


_VECTORCLOCK.fields_by_name['entries'].message_type = _CLOCKENTRY
_VERSIONED.fields_by_name['version'].message_type = _VECTORCLOCK
_ERROR.fields_by_name['version'].message_type = _VECTORCLOCK
_KEYEDVERSIONS.fields_by_name['versions'].message_type = _VERSIONED
_GETRESPONSE.fields_by_name['versioned'].message_type = _VERSIONED
_GETRESPONSE.fields_by_name['error'].message_type = _ERROR
_GETVERSIONRESPONSE.fields_by_name['versions'].message_type = _VECTORCLOCK
_GETVERSIONRESPONSE.fields_by_name['error'].message_type = _ERROR
_GETALLRESPONSE.fields_by_name['values'].message_type = _KEYEDVERSIONS
_GETALLRESPONSE.fields_by_name['error'].message_type = _ERROR
_PUTREQUEST.fields_by_name['versioned'].message_type = _VERSIONED
_PUTRESPONSE.fields_by_name['error'].message_type = _ERROR
_PUTRESPONSE.fields_by_name['version'].message_type = _VECTORCLOCK
_DELETEREQUEST.fields_by_name['version'].message_type = _VECTORCLOCK
_DELETERESPONSE.fields_by_name['error'].message_type = _ERROR
_VOLDEMORTREQUEST.fields_by_name['type'].enum_type = _REQUESTTYPE
_VOLDEMORTREQUEST.fields_by_name['get'].message_type = _GETREQUEST
_VOLDEMORTREQUEST.fields_by_name['getAll'].message_type = _GETALLREQUEST
_VOLDEMORTREQUEST.fields_by_name['put'].message_type = _PUTREQUEST
_VOLDEMORTREQUEST.fields_by_name['delete'].message_type = _DELETEREQUEST

class ClockEntry(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _CLOCKENTRY

class VectorClock(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _VECTORCLOCK

class Versioned(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _VERSIONED

class Error(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _ERROR

class KeyedVersions(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _KEYEDVERSIONS

class GetRequest(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _GETREQUEST

class GetResponse(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _GETRESPONSE

class GetVersionResponse(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _GETVERSIONRESPONSE

class GetAllRequest(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _GETALLREQUEST

class GetAllResponse(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _GETALLRESPONSE

class PutRequest(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _PUTREQUEST

class PutResponse(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _PUTRESPONSE

class DeleteRequest(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _DELETEREQUEST

class DeleteResponse(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _DELETERESPONSE

class VoldemortRequest(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _VOLDEMORTREQUEST

