/*
 * bittwiddlinghacks.hh
 *
 *  Created on: Dec 4, 2015
 *      Author: kalujny
 */

#ifndef BITTWIDDLINGHACKS_HH_
#define BITTWIDDLINGHACKS_HH_

int __attribute__ ((noinline,noclone)) fastLog2(unsigned int v);
int __attribute__ ((noinline,noclone)) fastLog2_64(unsigned long long value);
unsigned long long __attribute__ ((noinline,noclone)) rank64bitmsb(const unsigned long long v, const unsigned int pos = 1); // this accepts positions 1 - 64 and returns number of bits set from MSB up to INCLUDING pos
unsigned int __attribute__ ((noinline,noclone)) select64bitmsb(const unsigned long long v, unsigned int r);

inline unsigned long long bits_in_char(unsigned char c)
{
  // We could make these ints.  The tradeoff is size (eg does it overwhelm
  // the cache?) vs efficiency in referencing sub-word-sized array elements.
  static __thread const char bits_in[256] =
  {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
  };
  return bits_in[c];
}

inline unsigned long long googlerank(const unsigned char *bm, unsigned long long pos)
{
    unsigned long long retval = 0;
  for (; pos > 8ULL; pos -= 8ULL)
      retval += bits_in_char(*bm++);
  return retval + bits_in_char(*bm & ((1ULL << pos)-1ULL));
}

//TODO: __builtin_popcnt

#endif /* BITTWIDDLINGHACKS_HH_ */
