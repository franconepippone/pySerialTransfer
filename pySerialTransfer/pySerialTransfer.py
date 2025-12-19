from typing import Callable, Union, Iterable, Tuple, Any, cast

import logging
import os
import json
import struct
from enum import Enum
from typing import Union

import serial
import serial.tools.list_ports
from array import array
from .CRC import CRC

class InvalidSerialPort(Exception):
    pass

type SerializableObj = Union[float, int, str, dict[str, SerializableObj], bool]

def _serialize_value(val: SerializableObj, override: str | None = None) -> Tuple[Any, str]:
    '''
    Utility function for serializing values. Raises TypeError if an object
    type is not supported
    
    :param self: Description
    :param val: Description
    :type val: SerializableObj
    :param override: Description
    :type override: str | None
    '''
    if override:
        return val, override

    if isinstance(val, str):
        b = val.encode()
        return b, f"{len(b)}s"

    if isinstance(val, bool):
        return val, "?"

    if isinstance(val, int):
        return val, "i"

    if isinstance(val, float):
        return val, "f"
    
    if isinstance(val, dict):
        b = json.dumps(val).encode()
        return b, f"{len(b)}s"

    raise TypeError


class Status(Enum):
    CONTINUE        = 3
    NEW_DATA        = 2
    NO_DATA         = 1
    CRC_ERROR       = 0
    PAYLOAD_ERROR   = -1
    STOP_BYTE_ERROR = -2

ERROR_STATUS_SET = set((Status.CRC_ERROR, Status.PAYLOAD_ERROR, Status.STOP_BYTE_ERROR))

START_BYTE = 0x7E
STOP_BYTE  = 0x81

MAX_PAYLOAD_SIZE = 0xFE
MAX_PACKET_SIZE = 4 + MAX_PAYLOAD_SIZE + 2 # size of a full packet

BYTE_FORMATS = {'native':          '@',
                'native_standard': '=',
                'little-endian':   '<',
                'big-endian':      '>',
                'network':         '!'}

STRUCT_FORMAT_LENGTHS = {'c': 1,
                         'b': 1,
                         'B': 1,
                         '?': 1,
                         'h': 2,
                         'H': 2,
                         'i': 4,
                         'I': 4,
                         'l': 4,
                         'L': 4,
                         'q': 8,
                         'Q': 8,
                         'e': 2,
                         'f': 4,
                         'd': 8}

ARRAY_FORMAT_LENGTHS = {'b': 1,
                        'B': 1,
                        'u': 2,
                        'h': 2,
                        'H': 2,
                        'i': 2,
                        'I': 2,
                        'l': 4,
                        'q': 8,
                        'Q': 8,
                        'f': 4,
                        'd': 8}


class State(Enum):
    FIND_START_BYTE    = 0
    FIND_ID_BYTE       = 1
    FIND_OVERHEAD_BYTE = 2
    FIND_PAYLOAD_LEN   = 3
    FIND_PAYLOAD       = 4
    FIND_CRC           = 5
    FIND_END_BYTE      = 6


def constrain(val: int, min_: int, max_: int) -> int:
    if val < min_:
        return min_
    elif val > max_:
        return max_
    return val


def serial_ports():
    return [p.device for p in serial.tools.list_ports.comports(include_links=True)]

# callback passet to bind_callback(), called when using 'tick()'
type rcvCallback = Callable[[SerialTransfer], None]

class SerialTransfer:
    def __init__(self, 
            port: str, 
            baud: int=115200, 
            restrict_ports: bool = True, 
            debug: bool = True, 
            byte_format: str =BYTE_FORMATS['little-endian'], 
            timeout: float =0.05, 
            write_timeout: float | None = None
        ):
        '''
        Description:
        ------------
        Initialize transfer class and connect to the specified USB device

        :param port: int or str - port the USB device is connected to
        :param baud: int        - baud (bits per sec) the device is configured for
        :param restrict_ports: bool - only allow port selection from auto
                                      detected list
        :param byte_format:    str  - format for values packed/unpacked via the
                                      struct package as defined by
                                      https://docs.python.org/3/library/struct.html#struct-format-strings
        :param timeout:       float - timeout (in s) to set on pySerial for maximum wait for a read from the OS
                                      default 50ms marries up with DEFAULT_TIMEOUT in SerialTransfer
        :param write_timeout: float - timeout (in s) to set on pySerial for maximum wait for a write operation to the serial port
                                      default None causes no write timeouts to be raised
        :return: void
        '''

        self.bytes_to_rec: int = 0
        self.pay_index: int = 0
        self.rec_overhead_byte: int = 0
        # TODO review the indices
        self._tx_packet_buff = bytearray(MAX_PACKET_SIZE) # this contains the whole outbound packet
        self._txpackbuff_mv = memoryview(self._tx_packet_buff) # we create this once, and use it for each call of send()
        self.tx_buff = memoryview(self._tx_packet_buff)[4:-2] # this is a reference to just the payload bytes

        # this for now is unused, but rx_buff acts the same way
        self._rx_packet_buff = bytearray(MAX_PACKET_SIZE)
        self._rxpackbuff_mv = memoryview(self._rx_packet_buff)
        self.rx_buff = memoryview(self._rx_packet_buff)[4:-2] #[' '] * MAX_PACKET_SIZE

        self.debug: bool = debug
        self.id_byte: int = 0
        self.bytes_read: int = 0
        self.status: Status = Status.CRC_ERROR
        self.overhead_byte = 0xFF
        self.callbacks: dict[int, rcvCallback] = {}
        self.byte_format: str = byte_format

        self.state: State = State.FIND_START_BYTE
        
        if restrict_ports:
            self.port_name = None
            for p in serial_ports():
                if p == port or os.path.split(p)[-1] == port:
                    self.port_name = p
                    break

            if self.port_name is None:
                raise InvalidSerialPort('Invalid serial port specified.\
                    Valid options are {ports},  but {port} was provided'.format(
                    **{'ports': serial_ports(), 'port': port}))
        else:
            self.port_name = port

        self.crc = CRC()
        self.connection = serial.Serial()
        self.connection.port = self.port_name
        self.connection.baudrate = baud
        self.connection.timeout = timeout
        self.connection.write_timeout = write_timeout

    def open(self) -> bool:
        '''
        Description:
        ------------
        Open serial port and connect to device if possible

        :return: bool - True if successful, else False
        '''

        if not self.connection.is_open:
            try:
                self.connection.open()
                return True
            except serial.SerialException as e:
                logging.exception(e)
                return False
        return True
    
    def bind_callback(self, pack_id: int, cb: rcvCallback) -> None:
        '''
        Description:
        ------------
        Bind a callback function for a given packed id. This is called
        automatically when a new packet is fully parsed when using self.tick()
        
        :param self: Description
        :param pack_id: Description
        :type pack_id: int
        :param cb: Description
        :type cb: rcvCallback
        '''

        assert 0 <= pack_id <= 255, "Id must be a byte, in range [0, 255]"

        self.callbacks[pack_id] = cb

    def close(self):
        '''
        Description:
        ------------
        Close serial port

        :return: void
        '''
        if self.connection.is_open:
            self.connection.close()
    
    def tx_objs(self, 
        vals: Iterable[SerializableObj],
        start_pos: int = 0,
        byte_format: str = '',
        val_type_override: str = ''
        ) -> int | None:
        '''
        Insert a list of serializable objects into the rx buffer
        
        :param vals:     Iterable of serializable objects to insert into the tx buffer
        :type vals: Iterable[SerializableObj]
        :param start_pos:   int - index of TX buffer where the first byte
                                  of the value is to be stored in
        :param byte_format: str - byte order, size and alignment according to
                                  https://docs.python.org/3/library/struct.html#struct-format-strings
        :param val_type_override: str - manually specify format according to
                                        https://docs.python.org/3/library/struct.html#format-characters
    
        :return: int - index of the last byte of the value in the TX buffer + 1,
                       None if operation failed
        '''

        for value in vals:
            next_pos_candidate = self.tx_obj(value, start_pos, byte_format=byte_format, val_type_override=val_type_override)
            if next_pos_candidate is None:
                return None
            start_pos = next_pos_candidate

    def tx_obj(self, 
            val: SerializableObj, 
            start_pos: int = 0, 
            byte_format: str = '', 
            val_type_override: str = ''
        ) -> int | None:
        '''
        Description:
        -----------
        Insert an arbitrary variable's value into the TX buffer starting at the
        specified index
        
        :param val:         SerializableObj - value to be inserted into TX buffer
        :param start_pos:   int - index of TX buffer where the first byte
                                  of the value is to be stored in
        :param byte_format: str - byte order, size and alignment according to
                                  https://docs.python.org/3/library/struct.html#struct-format-strings
        :param val_type_override: str - manually specify format according to
                                        https://docs.python.org/3/library/struct.html#format-characters
    
        :return: int - index of the last byte of the value in the TX buffer + 1,
                       None if operation failed
        '''

        try:
            val, fmt = _serialize_value(val, val_type_override)
        except TypeError:
            return None

        # fmt_prefix is self.byte_format if byte_format is empty string
        fmt_prefix = byte_format or self.byte_format
        packed = struct.pack(fmt_prefix + fmt, val)

        return self.tx_bytes(packed, start_pos)
        
    def tx_bytes(self, val_bytes: bytes | bytearray, start_pos: int = 0) -> int:
        '''
        Description:
        -----------
        Insert a byte array into the TX buffer starting at the
        specified index
        
        :param val_bytes:   bytearray - value to be inserted into TX buffer
        :param start_pos:   int - index of TX buffer where the first byte
                                  of the value is to be stored in
        :return: int - index of the last byte of the value in the TX buffer + 1,
                       None if operation failed
        '''

        end_indx = start_pos + len(val_bytes)
        self.tx_buff[start_pos: end_indx] = val_bytes # much more efficient bytearray slice assignment
        return end_indx

    def rx_obj(self, 
            obj_type: type | str, 
            start_pos: int = 0,
            obj_byte_size: int = 0,
            list_format: str | None = None, 
            byte_format: str = ''
        ) -> Any:
        '''
        Description:
        ------------
        Extract an arbitrary variable's value from the RX buffer starting at
        the specified index. If object_type is list, it is assumed that the
        list to be extracted has homogeneous element types where the common
        element type can neither be list, dict, nor string longer than a
        single char
        
        :param obj_type:      type or str - type of object to extract from the
                                            RX buffer or format string as
                                            defined by https://docs.python.org/3/library/struct.html#format-characters
        :param start_pos:     int  - index of TX buffer where the first byte
                                     of the value is to be stored in
        :param obj_byte_size: int  - number of bytes making up extracted object
        :param list_format:   char - array.array format char to represent the
                                     common list element type as defined by
                                     https://docs.python.org/3/library/array.html#module-array
        :param byte_format: str    - byte order, size and alignment according to
                                     https://docs.python.org/3/library/struct.html#struct-format-strings
    
        :return unpacked_response: obj - object extracted from the RX buffer,
                                         None if operation failed
        '''
        
        if (obj_type == str) or (obj_type == dict):
            buff = bytes(self.rx_buff[start_pos:(start_pos + obj_byte_size)])
            format_str = '%ds' % len(buff)
            
        elif obj_type == float:
            format_str = 'f'
            buff = bytes(self.rx_buff[start_pos:(start_pos + STRUCT_FORMAT_LENGTHS[format_str])])
            
        elif obj_type == int:
            format_str = 'i'
            buff = bytes(self.rx_buff[start_pos:(start_pos + STRUCT_FORMAT_LENGTHS[format_str])])
            
        elif obj_type == bool:
            format_str = '?'
            buff = bytes(self.rx_buff[start_pos:(start_pos + STRUCT_FORMAT_LENGTHS[format_str])])
            
        elif obj_type == list:
            buff = bytes(self.rx_buff[start_pos:(start_pos + obj_byte_size)])
            
            if list_format:
                arr = array(list_format, buff)
                return arr.tolist()
            
            else:
                return None
        
        elif isinstance(obj_type, str):
            buff = bytes(self.rx_buff[start_pos:(start_pos + STRUCT_FORMAT_LENGTHS[obj_type])])
            format_str = obj_type
        
        else:
            return None
        
        if byte_format:
            unpacked_response = struct.unpack(byte_format + format_str, buff)[0]
            
        else:
            unpacked_response = struct.unpack(self.byte_format + format_str, buff)[0]
        
        if (obj_type == str) or (obj_type == dict):
            # remove any trailing bytes of value 0 from data
            if 0 in unpacked_response:
                unpacked_response = unpacked_response[:unpacked_response.index(0)]

            unpacked_response = unpacked_response.decode('utf-8')
        
        if obj_type == dict:
            unpacked_response = json.loads(unpacked_response)
        
        return unpacked_response

    def rx_payload_bytes(self) -> memoryview[int]:
        '''
        :return: Returns a memoryview of the payload bytes of the latest received packet.
            You can treat this as a standard bytes object for most scenarios.
        :rtype: memoryview[int]
        '''
        return self.rx_buff[:self.bytes_to_rec]


    def calc_overhead(self, pay_len):
        '''
        Description:
        ------------
        Calculates the COBS (Consistent Overhead Stuffing) Overhead
        byte and stores it in the class's overhead_byte variable. This
        variable holds the byte position (within the payload) of the
        first payload byte equal to that of START_BYTE

        :param pay_len: int - number of bytes in the payload

        :return: void
        '''

        self.overhead_byte = 0xFF

        for i in range(pay_len):
            if self.tx_buff[i] == START_BYTE:
                self.overhead_byte = i
                break

    def find_last(self, pay_len):
        '''
        Description:
        ------------
        Finds last instance of the value START_BYTE within the given
        packet array

        :param pay_len: int - number of bytes in the payload

        :return: int - location of the last instance of the value START_BYTE
                       within the given packet array
        '''

        if pay_len <= MAX_PAYLOAD_SIZE:
            for i in range(pay_len - 1, -1, -1):
                if self.tx_buff[i] == START_BYTE:
                    return i
        return -1

    def stuff_packet(self, pay_len: int):
        '''
        Description:
        ------------
        Enforces the COBS (Consistent Overhead Stuffing) ruleset across
        all bytes in the packet against the value of START_BYTE

        :param pay_len: int - number of bytes in the payload

        :return: void
        '''

        ref_byte = self.find_last(pay_len)

        if (not ref_byte == -1) and (ref_byte <= MAX_PAYLOAD_SIZE):
            for i in range(pay_len - 1, -1, -1):
                if self.tx_buff[i] == START_BYTE:
                    self.tx_buff[i] = ref_byte - i
                    ref_byte = i

    def send(self, message_len: int, packet_id: int = 0):
        '''
        Description:
        ------------
        Send a specified number of bytes in packetized form

        :param message_len: int - number of bytes from the tx_buff to send as
                                  payload in the packet
        :param packet_id:   int - ID of the packet to send                                  

        :return: bool - whether or not the operation was successful
        '''

        message_len = constrain(message_len, 0, MAX_PAYLOAD_SIZE)

        try:
            self.calc_overhead(message_len)
            self.stuff_packet(message_len)
            found_checksum = self.crc.calculate(self.tx_buff, message_len)

            self._tx_packet_buff[0] = START_BYTE    # NOTE this can be done during __init__
            self._tx_packet_buff[1] = packet_id
            self._tx_packet_buff[2] = self.overhead_byte
            self._tx_packet_buff[3] = message_len

            # no copy necessary, payload bytes are already inside _tx_packet_buff

            self._tx_packet_buff[message_len + 4] = found_checksum
            self._tx_packet_buff[message_len + 5] = STOP_BYTE

            # fast - zero copy. We use the memoryview object we created during __init__
            packet_bytes = self._txpackbuff_mv[0:message_len + 6]
            
            if self.open():
                self.connection.write(packet_bytes)

            return True

        except Exception:
            import traceback
            traceback.print_exc()

            return False

    def unpack_packet(self):
        '''
        Description:
        ------------
        Unpacks all COBS-stuffed bytes within the array

        :return: void
        '''

        test_index = self.rec_overhead_byte

        if test_index <= MAX_PAYLOAD_SIZE:
            while self.rx_buff[test_index]:
                delta = self.rx_buff[test_index]
                self.rx_buff[test_index] = START_BYTE
                test_index += delta

            self.rx_buff[test_index] = START_BYTE
    
    def available(self):
        '''
        Description:
        ------------
        Parses incoming serial data, analyzes packet contents,
        and reports errors/successful packet reception

        :return self.bytes_read: int - number of bytes read from the received
                                      packet
        '''

        if self.open():
            if self.connection.in_waiting:
                while self.connection.in_waiting:
                    rec_char = self.connection.read(1)[0]

                    if self.state == State.FIND_START_BYTE:
                        if rec_char == START_BYTE:
                            self.state = State.FIND_ID_BYTE
                    
                    elif self.state == State.FIND_ID_BYTE:
                        self.id_byte = rec_char
                        self.state = State.FIND_OVERHEAD_BYTE

                    elif self.state == State.FIND_OVERHEAD_BYTE:
                        self.rec_overhead_byte = rec_char
                        self.state = State.FIND_PAYLOAD_LEN

                    elif self.state == State.FIND_PAYLOAD_LEN:
                        if rec_char > 0 and rec_char <= MAX_PACKET_SIZE:
                            self.bytes_to_rec = rec_char
                            self.pay_index = 0
                            self.state = State.FIND_PAYLOAD
                        else:
                            self.bytes_read = 0
                            self.state = State.FIND_START_BYTE
                            self.status = Status.PAYLOAD_ERROR
                            return self.bytes_read

                    elif self.state == State.FIND_PAYLOAD:
                        if self.pay_index < self.bytes_to_rec:
                            self.rx_buff[self.pay_index] = rec_char
                            self.pay_index += 1

                            # Try to receive as many more bytes as we can, but we might not get all of them
                            # if there is a timeout from the OS
                            if self.pay_index != self.bytes_to_rec:
                                more_bytes = self.connection.read(self.bytes_to_rec - self.pay_index)
                                next_index = self.pay_index + len(more_bytes)

                                self.rx_buff[self.pay_index:next_index] = more_bytes
                                self.pay_index = next_index

                            if self.pay_index == self.bytes_to_rec:
                                self.state = State.FIND_CRC

                    elif self.state == State.FIND_CRC:
                        found_checksum = self.crc.calculate(
                            self.rx_buff, self.bytes_to_rec)

                        if found_checksum == rec_char:
                            self.state = State.FIND_END_BYTE
                        else:
                            self.bytes_read = 0
                            self.state = State.FIND_START_BYTE
                            self.status = Status.CRC_ERROR
                            return self.bytes_read

                    elif self.state == State.FIND_END_BYTE:
                        self.state = State.FIND_START_BYTE

                        if rec_char == STOP_BYTE:
                            self.unpack_packet()
                            self.bytes_read = self.bytes_to_rec
                            self.status = Status.NEW_DATA
                            return self.bytes_read

                        self.bytes_read = 0
                        self.status = Status.STOP_BYTE_ERROR
                        return self.bytes_read

                    else:
                        logging.error('Undefined state: {}'.format(self.state))

                        self.bytes_read = 0
                        self.state = State.FIND_START_BYTE
                        return self.bytes_read
            else:
                self.bytes_read = 0
                self.status = Status.NO_DATA
                return self.bytes_read

        self.bytes_read = 0
        self.status = Status.CONTINUE
        return self.bytes_read
    
    def tick(self) -> bool:
        '''
        Description:
        ------------
        Automatically parse all incoming packets, print debug statements if
        necessary (if enabled), and call the callback function that corresponds
        to the parsed packet's ID (if such a callback exists for that packet
        ID)

        :return: bool
        '''
        
        if self.available() > 0:
            if self.id_byte in self.callbacks:
                self.callbacks[self.id_byte](self)
            elif self.debug:
                logging.error('No callback available for packet ID {}'.format(self.id_byte))
            
            return True
        
        elif self.debug and self.status in ERROR_STATUS_SET:
            logging.error(self.status.name)
        
        return False
