/*
 * bittwiddlinghacks.cpp
 *
 *  Created on: Dec 4, 2015
 *      Author: kalujny
 */

#include "bittwiddlinghacks.hh"

// below 3 functions taken whole from bit twiddling hacks. Thanks go to smart people at AMD, etc..
static const int MultiplyDeBruijnBitPosition[32] =
{
  0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30,
  8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31
};

int __attribute__ ((noinline,noclone)) fastLog2(unsigned int v)
{
    int r;      // result goes here

    v |= v >> 1; // first round down to one less than a power of 2
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;

    r = MultiplyDeBruijnBitPosition[(unsigned int)(v * 0x07C4ACDDU) >> 27];
    return r;
}

static const int MultiplyDeBruijnBitPosition64[64] = {
    63,  0, 58,  1, 59, 47, 53,  2,
    60, 39, 48, 27, 54, 33, 42,  3,
    61, 51, 37, 40, 49, 18, 28, 20,
    55, 30, 34, 11, 43, 14, 22,  4,
    62, 57, 46, 52, 38, 26, 32, 41,
    50, 36, 17, 19, 29, 10, 13, 21,
    56, 45, 25, 31, 35, 16,  9, 12,
    44, 24, 15,  8, 23,  7,  6,  5};

int __attribute__ ((noinline,noclone)) fastLog2_64(unsigned long long value)
{
    value |= value >> 1;
    value |= value >> 2;
    value |= value >> 4;
    value |= value >> 8;
    value |= value >> 16;
    value |= value >> 32;
    return MultiplyDeBruijnBitPosition64[((unsigned long long)((value - (value >> 1))*0x07EDD5E59A4E28C2)) >> 58];
}

unsigned long long __attribute__ ((noinline,noclone)) rank64bitmsb(const unsigned long long v, const unsigned int pos)
{
     unsigned long long r;       // Resulting rank of bit at pos goes here.

     // Shift out bits after given position.
     r = v >> (sizeof(v) * 8 - pos);
     // Count set bits in parallel.
     // r = (r & 0x5555...) + ((r >> 1) & 0x5555...);
     r = r - ((r >> 1) & ~0UL/3);
     // r = (r & 0x3333...) + ((r >> 2) & 0x3333...);
     r = (r & ~0UL/5) + ((r >> 2) & ~0UL/5);
     // r = (r & 0x0f0f...) + ((r >> 4) & 0x0f0f...);
     r = (r + (r >> 4)) & ~0UL/17;
     // r = r % 255;
     r = (r * (~0UL/255)) >> ((sizeof(v) - 1) * 8);
     return r;
}

unsigned int __attribute__ ((noinline,noclone)) select64bitmsb(const unsigned long long v, unsigned int r)
{
    unsigned int s;      // Output: Resulting position of bit with rank r [1-64]
    unsigned int t;      // Bit count temporary.

    // Do a normal parallel bit count for a 64-bit integer,
    // but store all intermediate steps.
    // a = (v & 0x5555...) + ((v >> 1) & 0x5555...);
    const unsigned long long a =  v - ((v >> 1) & ~0UL/3);
    // b = (a & 0x3333...) + ((a >> 2) & 0x3333...);
    const unsigned long long b = (a & ~0UL/5) + ((a >> 2) & ~0UL/5);
    // c = (b & 0x0f0f...) + ((b >> 4) & 0x0f0f...);
    const unsigned long long c = (b + (b >> 4)) & ~0UL/0x11;
    // d = (c & 0x00ff...) + ((c >> 8) & 0x00ff...);
    const unsigned long long d = (c + (c >> 8)) & ~0UL/0x101;
    t = (d >> 32) + (d >> 48);
    // Now do branchless select!
    s  = 64;
    // if (r > t) {s -= 32; r -= t;}
    s -= ((t - r) & 256) >> 3; r -= (t & ((t - r) >> 8));
    t  = (d >> (s - 16)) & 0xff;
    // if (r > t) {s -= 16; r -= t;}
    s -= ((t - r) & 256) >> 4; r -= (t & ((t - r) >> 8));
    t  = (c >> (s - 8)) & 0xf;
    // if (r > t) {s -= 8; r -= t;}
    s -= ((t - r) & 256) >> 5; r -= (t & ((t - r) >> 8));
    t  = (b >> (s - 4)) & 0x7;
    // if (r > t) {s -= 4; r -= t;}
    s -= ((t - r) & 256) >> 6; r -= (t & ((t - r) >> 8));
    t  = (a >> (s - 2)) & 0x3;
    // if (r > t) {s -= 2; r -= t;}
    s -= ((t - r) & 256) >> 7; r -= (t & ((t - r) >> 8));
    t  = (v >> (s - 1)) & 0x1;
    // if (r > t) s--;
    s -= ((t - r) & 256) >> 8;
    s = 65 - s;
    return s;
}



