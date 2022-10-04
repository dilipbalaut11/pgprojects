#ifndef PG_VARINT_H
#define PG_VARINT_H

/*
 * Variable length integer encoding.
 *
 * Represent the length of the varint, in bytes - 1, as the number of
 * "leading" 0 bits in the first byte. I.e. the length is stored in
 * unary.  If less than 9 bytes of data are needed, the leading 0 bits
 * are followed by a 1 bit, followed by the data. When encoding 64bit
 * integers, for 8 bytes of input data, there is however no high 1 bit
 * in the second output byte, as that would be unnecessary (and
 * increase the size to 10 bytes of output bytes). If, hypothetically,
 * larger numbers were supported, that'd not be the case, obviously.
 *
 * The reason to encode the number of bytes as zero bits, rathter than
 * ones, is that more architectures have instructions for efficiently
 * finding the first bit set, than finding the first bit unset.
 *
 * In contrast to encoding the length as e.g. the high-bit in each following
 * bytes, this allows for faster decoding (and encoding, although the benefit
 * is smaller), because no length related branches are needed after processing
 * the first byte and no 7 bit/byte -> 8 bit/byte conversion is needed. The
 * mask / shift overhead is similar.
 *
 *
 * I.e. the encoding of a unsiged 64bit integer is as follows
 * [0 {#bytes - 1}][1 as separator][leading 0 bits][data]
 *
 * E.g.
 * - 7 (input 0b0000'0111) is encoded as 0b1000'0111
 * - 145 (input 0b1001'0001) is 0b0100'0000''1001'0001 (as the separator does
 *   not fit into the one byte of the fixed width encoding)
 * - 4141 (input 0b0001'0000''0010'1101) is 0b0101'0000''0010'1101 (the
 *   leading 0, followed by a 1 indicating that two bytes are needed)
 *
 *
 * Naively encoding negative two's complement integers this way would
 * lead to such numbers always being stored as the maximum varint
 * length, due to the leading sign bits.  Instead the sign bit is
 * encoded in the least significant bit. To avoid different limits
 * than native 64bit integers - in particular around the lowest
 * possible value - the data portion is bit flipped instead of
 * negated.
 *
 * The reason for the sign bit to be trailing is that that makes it
 * trivial to reuse code of the unsigned case, whereas a leading bit
 * would make that harder.
 *
 * XXX: One problem with storing negative numbers this way is that it
 * makes varints not compare lexicographically. Is it worth the code
 * duplication to allow for that? Given the variable width of
 * integers, lexicographic sorting doesn't seem that relevant.
 *
 * Therefore the encoding of a signed 64bit integer is as follows:
 * [0 {#bytes - 1}][1][leading 0 bits][data][sign bit]
 *
 * E.g. the encoding of
 * -  7 (input 0b0000'0111) is encoded as 0b1000'1110
 * - -7 (input 0b[1*]''1111'1000) is encoded as 0b1001'1101
 */

/*
 * TODO:
 * - check how well this works on big endian
 * - consider encoding as little endian, more common
 * - proper header separation
 */


/*
 * The max value fitting in a 1 byte varint. One bit is needed for the length
 * indicator.
 */
#define PG_VARINT_UINT64_MAX_1BYTE_VAL ((1L << (BITS_PER_BYTE - 1)) - 1)

/*
 * The max value fitting into a varint below 8 bytes has to take into account
 * the number of bits for the length indicator for 8 bytes (7 bits), and
 * additionally a bit for the separator (1 bit).
 */
#define PG_VARINT_UINT64_MAX_8BYTE_VAL ((1L << (64 - (7 + 1))) - 1)


extern int pg_varint_encode_uint64(uint64 uval, uint8 *buf);
extern int pg_varint_encode_int64(int64 val, uint8 *buf);

extern uint64 pg_varint_decode_uint64(const uint8 *buf, int *consumed);
extern int64 pg_varint_decode_int64(const uint8 *buf, int *consumed);


#endif /* PG_VARINT_H */
