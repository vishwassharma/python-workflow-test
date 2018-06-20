from ctypes import *
import json


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


def main(logger=None, filename='test.bin', save_filename=""):
    heartbeat = {}  # TODO: ask about the frequency of heartbeat message and its usage
    arr = []
    counter = 0
    infile = open(filename, 'rb')
    bin_data = infile.read()
    header = StreamHeader()
    header_size = sizeof(StreamHeader)
    # import pdb; pdb.set_trace()
    while bin_data:
        counter += 1
        iter_data = {}
        # Abuse ctypes to initialize from a string
        bb = create_string_buffer(bin_data)
        memmove(addressof(header), addressof(bb), header_size)

        iter_data['msg_type'] = header.msg_type
        iter_data['seq_no'] = header.seq_no

        if header.msg_type == 'Z':
            # print "Received HeartBeat Message"
            bb = create_string_buffer(bin_data[header_size:])
            packet = HeartBeatPayload()
            memmove(addressof(packet), addressof(bb), sizeof(packet))
            # print "Heartbeat {} sq:{}".format(header.msg_type, packet.last_seq_no)

            heartbeat['msg_type'] = 'Z'
            heartbeat['last_seq_no'] = packet.last_seq_no

        elif header.msg_type == 'N' or header.msg_type == 'X' or header.msg_type == 'M':
            # print "Received Order Message"
            bb = create_string_buffer(bin_data[header_size:])
            packet = OrderPayload()
            memmove(addressof(packet), addressof(bb), sizeof(packet))
            iter_data['order_id'] = packet.order_id
            iter_data['token'] = packet.token
            iter_data['order_type'] = packet.order_type
            iter_data['price'] = packet.price
            iter_data['quantity'] = packet.quantity
            iter_data['timestamp'] = packet.timestamp

        elif header.msg_type == 'T':
            # print "Received Trade Message"
            bb = create_string_buffer(bin_data[header_size:])
            packet = TradePayload()
            memmove(addressof(packet), addressof(bb), sizeof(packet))
            iter_data['buy_order_id'] = packet.buy_order_id
            iter_data['sell_order_id'] = packet.sell_order_id
            iter_data['token'] = packet.token
            iter_data['price'] = packet.price
            iter_data['quantity'] = packet.quantity
            iter_data['timestamp'] = packet.timestamp

        elif header.msg_type == 'G' or header.msg_type == 'H' or header.msg_type == 'J':
            # print "Received Spread Order Message"
            bb = create_string_buffer(bin_data[header_size:])
            packet = OrderPayload()
            memmove(addressof(packet), addressof(bb), sizeof(packet))
            iter_data['order_id'] = packet.order_id
            iter_data['token'] = packet.token
            iter_data['order_type'] = packet.order_type
            iter_data['price'] = packet.price
            iter_data['quantity'] = packet.quantity
            iter_data['timestamp'] = packet.timestamp

        elif header.msg_type == 'K':
            # print "Received Spread Trade Message"
            bb = create_string_buffer(bin_data[header_size:])
            packet = TradePayload()
            memmove(addressof(packet), addressof(bb), sizeof(packet))
            iter_data['buy_order_id'] = packet.buy_order_id
            iter_data['sell_order_id'] = packet.sell_order_id
            iter_data['token'] = packet.token
            iter_data['price'] = packet.price
            iter_data['quantity'] = packet.quantity
            iter_data['timestamp'] = packet.timestamp

        else:
            # print (header.msg_type)
            pass
            # TODO: ask about dealing with 'Y' messages

        bin_data = bin_data[header.msg_len:]
        arr.append(iter_data)

        if counter % 500 == 0 and bin_data and logger:
            print ("File: {}, count: {}".format(filename, counter))

    json.dump({
        'heartbeat': heartbeat,
        'data': arr,
    }, open(save_filename or filename.replace('.bin', '.json'), 'w'))


if __name__ == "__main__":
    main()