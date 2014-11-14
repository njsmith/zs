/**
 * This file has been hand-modified to convert 'static inline' functions into
 * 'static', because 'static inline' is unsupported by MSVC.
 */

/**
 * \file pycrc-crc64xz.h
 * Functions and types for CRC checks.
 *
 * Generated on Tue Apr  1 19:09:25 2014,
 * by pycrc v0.8.1, http://www.tty1.net/pycrc/
 * using the configuration:
 *    Width        = 64
 *    Poly         = 0x42f0e1eba9ea3693
 *    XorIn        = 0xffffffffffffffff
 *    ReflectIn    = True
 *    XorOut       = 0xffffffffffffffff
 *    ReflectOut   = True
 *    Algorithm    = table-driven
 *****************************************************************************/
#ifndef __PYCRC_CRC64XZ_H__
#define __PYCRC_CRC64XZ_H__

#include <stdlib.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif


/**
 * The definition of the used algorithm.
 *****************************************************************************/
#define CRC_ALGO_TABLE_DRIVEN 1


/**
 * The type of the CRC values.
 *
 * This type must be big enough to contain at least 64 bits.
 *****************************************************************************/
typedef uint_fast64_t pycrc_crc64xz_t;


/**
 * Reflect all bits of a \a data word of \a data_len bytes.
 *
 * \param data         The data word to be reflected.
 * \param data_len     The width of \a data expressed in number of bits.
 * \return             The reflected data.
 *****************************************************************************/
pycrc_crc64xz_t pycrc_crc64xz_reflect(pycrc_crc64xz_t data, size_t data_len);


/**
 * Calculate the initial crc value.
 *
 * \return     The initial crc value.
 *****************************************************************************/
static pycrc_crc64xz_t pycrc_crc64xz_init(void)
{
    return 0xffffffffffffffff;
}


/**
 * Update the crc value with new data.
 *
 * \param crc      The current crc value.
 * \param data     Pointer to a buffer of \a data_len bytes.
 * \param data_len Number of bytes in the \a data buffer.
 * \return         The updated crc value.
 *****************************************************************************/
pycrc_crc64xz_t pycrc_crc64xz_update(pycrc_crc64xz_t crc, const unsigned char *data, size_t data_len);


/**
 * Calculate the final crc value.
 *
 * \param crc  The current crc value.
 * \return     The final crc value.
 *****************************************************************************/
static pycrc_crc64xz_t pycrc_crc64xz_finalize(pycrc_crc64xz_t crc)
{
    return crc ^ 0xffffffffffffffff;
}


#ifdef __cplusplus
}           /* closing brace for extern "C" */
#endif

#endif      /* __PYCRC_CRC64XZ_H__ */
