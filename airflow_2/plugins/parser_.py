from ctypes import *


class OrderPayload(LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ('timestamp', c_int64),
        ('order_id', c_double),
        ('token', c_int),
        ('order_type', c_char),
        ('price', c_int),
        ('quantity', c_int),
    ]


class TradePayload(LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ('timestamp', c_int64),
        ('buy_order_id', c_double),
        ('sell_order_id', c_double),
        ('token', c_int),
        ('price', c_int),
        ('quantity', c_int),
    ]


class HeartBeatPayload(LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ('last_seq_no', c_int),
    ]


class StreamHeader(LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ('msg_len', c_short),
        ('stream_id', c_short),
        ('seq_no', c_int),
        ('msg_type', c_char)
    ]


infile = open('test.bin', 'rb')
data = infile.read()
header = StreamHeader()
headerSize = sizeof(StreamHeader)
# import pdb; pdb.set_trace()
while data:
    # data, sender = s.recvfrom(1500)

    # Abuse ctypes to initialize from a string
    bb = create_string_buffer(data)
    # print addressof(bb)
    # print(type(bb))
    memmove(addressof(header), addressof(bb), headerSize)

    # a = cast(bb, POINTER(header))
    # aPtr = cast(pointer(a), POINTER(c_void_p))
    # aPtr.contents.value += sizeof(headerSize)

    if (header.msg_type == 'Z'):
        # print "Received HeartBeat Message"
        bb = create_string_buffer(data[headerSize:])
        packet = HeartBeatPayload()
        memmove(addressof(packet), addressof(bb), sizeof(packet))
        print "Heartbeat {} sq:{}".format(header.msg_type, packet.last_seq_no)
    elif (header.msg_type == 'N' or header.msg_type == 'X' or header.msg_type == 'M'):
        # print "Received Order Message"
        bb = create_string_buffer(data[headerSize:])
        packet = OrderPayload()
        memmove(addressof(packet), addressof(bb), sizeof(packet))
        print "Order {} sq:{} id:{:.1f} to:{} ot:{} p:{} q:{}".format(header.msg_type, header.seq_no, packet.order_id,
                                                                      packet.token, packet.order_type, packet.price,
                                                                      packet.quantity)
    elif (header.msg_type == 'T'):
        # print "Received Trade Message"
        bb = create_string_buffer(data[headerSize:])
        packet = TradePayload()
        memmove(addressof(packet), addressof(bb), sizeof(packet))
        print "Trade {} sq:{} bid:{:.1f} sid:{:.1f} to:{} p:{} q:{}".format(header.msg_type, header.seq_no,
                                                                            packet.buy_order_id, packet.sell_order_id,
                                                                            packet.token, packet.price, packet.quantity)
    elif (header.msg_type == 'G' or header.msg_type == 'H' or header.msg_type == 'J'):
        # print "Received Spread Order Message"
        bb = create_string_buffer(data[headerSize:])
        packet = OrderPayload()
        memmove(addressof(packet), addressof(bb), sizeof(packet))
        print "Spread Order {} sq:{} id:{:.1f} to:{} ot:{} p:{} q:{}".format(header.msg_type, header.seq_no,
                                                                             packet.order_id, packet.token,
                                                                             packet.order_type, packet.price,
                                                                             packet.quantity)
    elif (header.msg_type == 'K'):
        # print "Received Spread Trade Message"
        bb = create_string_buffer(data[headerSize:])
        packet = TradePayload()
        memmove(addressof(packet), addressof(bb), sizeof(packet))
        print "Spread Trade {} sq:{} bid:{:.1f} sid:{:.1f} to:{} p:{} q:{}".format(header.msg_type, header.seq_no,
                                                                                   packet.buy_order_id,
                                                                                   packet.sell_order_id, packet.token,
                                                                                   packet.price, packet.quantity)
    else:
        print header.msg_type

    data = data[header.msg_len:]

