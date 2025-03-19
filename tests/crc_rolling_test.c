#include <stdio.h>
#include <stdint.h>

#define kCrcPoly     0xEDB88320
#ifndef CRC_INIT_VAL
#define CRC_INIT_VAL 0xFFFFFFFF /* 0xFFFFFFFF for zip/rar/7-zip quasi-CRC */
#endif

// For rolling CRC hash demo
#define WINSIZE      4096
#define TESTSIZE     2000

// Fast CRC table construction algorithm
void FastTableBuild (unsigned CRCTable[256], unsigned seed)
{
  unsigned i, j, r;

  CRCTable[0]   = 0;
  CRCTable[128] = r = seed;
  for (i=64; i; i/=2)
    CRCTable[i] = r = (r >> 1) ^ (kCrcPoly & ~((r & 1) - 1));

  for (i=2; i<256; i*=2)
    for (j=1; j<i; j++)
      CRCTable[i+j] = CRCTable[i] ^ CRCTable[j];
}


#define init_CRC()                  CRC_INIT_VAL
#define update_CRC(crc,CRCTable,c)  (CRCTable[((crc)^(c)) & 0xFF] ^ ((crc)>>8))
#define finish_CRC(crc)             ((crc) ^ CRC_INIT_VAL)
// c = buffer[WINSIZE+i]) ^ RollingCRCTab[buffer[i]]
unsigned calcCRC (unsigned char *buffer, unsigned len, unsigned CRCTable[256])
{
  unsigned crc = init_CRC(), i;
  for (i=0; i<len; i++)
    crc = update_CRC(crc,CRCTable,buffer[i]);
  return finish_CRC(crc);
}


void BuildRollingCRCTable (const unsigned CRCTable[256], unsigned RollingCRCTable[256])
{
    unsigned i,c,x,y;
    for(c=0;c<256;c++)
    {
        x = init_CRC();
        y = init_CRC();
        x = update_CRC(x,CRCTable,c);
        y = update_CRC(y,CRCTable,0);
        for(i=0;i<WINSIZE-1;i++)
        {
            x = update_CRC(x,CRCTable,0);
            y = update_CRC(y,CRCTable,0);
        }
        x = update_CRC(x,CRCTable,0);
        RollingCRCTable[c] = x ^ y;
    }
}

int main()
{
  unsigned i, j, r,
      FastCRCTab[256], SlowRollingCRCTab[256], crc1, crc2;

  // Fast CRC table construction
  FastTableBuild (FastCRCTab, kCrcPoly);

  // Classic CRC table construction algorithm
  // for (i=0; i<256; i++)
  // {
  //   r = i;
  //   for (j = 0; j < 8; j++)
  //     r = (r >> 1) ^ (kCrcPoly & ~((r & 1) - 1));
  //   CRCTab[i] = r;
  //   if (FastCRCTab[i] != CRCTab[i])   // unit testing :)
  //     printf("c-crc: %02x %08x %08x\n", i, r, (r & 1) ? (r>>1)^kCrcPoly : (r>>1));
  // }

  unsigned CRCTab[256] = {
    0x00000000,0x77073096,0xEE0E612C,0x990951BA,0x076DC419,0x706AF48F,0xE963A535,0x9E6495A3,
    0x0EDB8832,0x79DCB8A4,0xE0D5E91E,0x97D2D988,0x09B64C2B,0x7EB17CBD,0xE7B82D07,0x90BF1D91,
    0x1DB71064,0x6AB020F2,0xF3B97148,0x84BE41DE,0x1ADAD47D,0x6DDDE4EB,0xF4D4B551,0x83D385C7,
    0x136C9856,0x646BA8C0,0xFD62F97A,0x8A65C9EC,0x14015C4F,0x63066CD9,0xFA0F3D63,0x8D080DF5,
    0x3B6E20C8,0x4C69105E,0xD56041E4,0xA2677172,0x3C03E4D1,0x4B04D447,0xD20D85FD,0xA50AB56B,
    0x35B5A8FA,0x42B2986C,0xDBBBC9D6,0xACBCF940,0x32D86CE3,0x45DF5C75,0xDCD60DCF,0xABD13D59,
    0x26D930AC,0x51DE003A,0xC8D75180,0xBFD06116,0x21B4F4B5,0x56B3C423,0xCFBA9599,0xB8BDA50F,
    0x2802B89E,0x5F058808,0xC60CD9B2,0xB10BE924,0x2F6F7C87,0x58684C11,0xC1611DAB,0xB6662D3D,
    0x76DC4190,0x01DB7106,0x98D220BC,0xEFD5102A,0x71B18589,0x06B6B51F,0x9FBFE4A5,0xE8B8D433,
    0x7807C9A2,0x0F00F934,0x9609A88E,0xE10E9818,0x7F6A0DBB,0x086D3D2D,0x91646C97,0xE6635C01,
    0x6B6B51F4,0x1C6C6162,0x856530D8,0xF262004E,0x6C0695ED,0x1B01A57B,0x8208F4C1,0xF50FC457,
    0x65B0D9C6,0x12B7E950,0x8BBEB8EA,0xFCB9887C,0x62DD1DDF,0x15DA2D49,0x8CD37CF3,0xFBD44C65,
    0x4DB26158,0x3AB551CE,0xA3BC0074,0xD4BB30E2,0x4ADFA541,0x3DD895D7,0xA4D1C46D,0xD3D6F4FB,
    0x4369E96A,0x346ED9FC,0xAD678846,0xDA60B8D0,0x44042D73,0x33031DE5,0xAA0A4C5F,0xDD0D7CC9,
    0x5005713C,0x270241AA,0xBE0B1010,0xC90C2086,0x5768B525,0x206F85B3,0xB966D409,0xCE61E49F,
    0x5EDEF90E,0x29D9C998,0xB0D09822,0xC7D7A8B4,0x59B33D17,0x2EB40D81,0xB7BD5C3B,0xC0BA6CAD,
    0xEDB88320,0x9ABFB3B6,0x03B6E20C,0x74B1D29A,0xEAD54739,0x9DD277AF,0x04DB2615,0x73DC1683,
    0xE3630B12,0x94643B84,0x0D6D6A3E,0x7A6A5AA8,0xE40ECF0B,0x9309FF9D,0x0A00AE27,0x7D079EB1,
    0xF00F9344,0x8708A3D2,0x1E01F268,0x6906C2FE,0xF762575D,0x806567CB,0x196C3671,0x6E6B06E7,
    0xFED41B76,0x89D32BE0,0x10DA7A5A,0x67DD4ACC,0xF9B9DF6F,0x8EBEEFF9,0x17B7BE43,0x60B08ED5,
    0xD6D6A3E8,0xA1D1937E,0x38D8C2C4,0x4FDFF252,0xD1BB67F1,0xA6BC5767,0x3FB506DD,0x48B2364B,
    0xD80D2BDA,0xAF0A1B4C,0x36034AF6,0x41047A60,0xDF60EFC3,0xA867DF55,0x316E8EEF,0x4669BE79,
    0xCB61B38C,0xBC66831A,0x256FD2A0,0x5268E236,0xCC0C7795,0xBB0B4703,0x220216B9,0x5505262F,
    0xC5BA3BBE,0xB2BD0B28,0x2BB45A92,0x5CB36A04,0xC2D7FFA7,0xB5D0CF31,0x2CD99E8B,0x5BDEAE1D,
    0x9B64C2B0,0xEC63F226,0x756AA39C,0x026D930A,0x9C0906A9,0xEB0E363F,0x72076785,0x05005713,
    0x95BF4A82,0xE2B87A14,0x7BB12BAE,0x0CB61B38,0x92D28E9B,0xE5D5BE0D,0x7CDCEFB7,0x0BDBDF21,
    0x86D3D2D4,0xF1D4E242,0x68DDB3F8,0x1FDA836E,0x81BE16CD,0xF6B9265B,0x6FB077E1,0x18B74777,
    0x88085AE6,0xFF0F6A70,0x66063BCA,0x11010B5C,0x8F659EFF,0xF862AE69,0x616BFFD3,0x166CCF45,
    0xA00AE278,0xD70DD2EE,0x4E048354,0x3903B3C2,0xA7672661,0xD06016F7,0x4969474D,0x3E6E77DB,
    0xAED16A4A,0xD9D65ADC,0x40DF0B66,0x37D83BF0,0xA9BCAE53,0xDEBB9EC5,0x47B2CF7F,0x30B5FFE9,
    0xBDBDF21C,0xCABAC28A,0x53B39330,0x24B4A3A6,0xBAD03605,0xCDD70693,0x54DE5729,0x23D967BF,
    0xB3667A2E,0xC4614AB8,0x5D681B02,0x2A6F2B94,0xB40BBE37,0xC30C8EA1,0x5A05DF1B,0x2D02EF8D,
  };

  unsigned RollingCRCTab[256] = {
  0x7f69d36e,  0x4deb6724,  0x1a6cbbfa,  0x28ee0fb0,  0xb5630246,  0x87e1b60c,  0xd0666ad2,  0xe2e4de98,
  0x300d777f,  0x028fc335,  0x55081feb,  0x678aaba1,  0xfa07a657,  0xc885121d,  0x9f02cec3,  0xad807a89,
  0xe1a09b4c,  0xd3222f06,  0x84a5f3d8,  0xb6274792,  0x2baa4a64,  0x1928fe2e,  0x4eaf22f0,  0x7c2d96ba,
  0xaec43f5d,  0x9c468b17,  0xcbc157c9,  0xf943e383,  0x64ceee75,  0x564c5a3f,  0x01cb86e1,  0x334932ab,
  0x998a456b,  0xab08f121,  0xfc8f2dff,  0xce0d99b5,  0x53809443,  0x61022009,  0x3685fcd7,  0x0407489d,
  0xd6eee17a,  0xe46c5530,  0xb3eb89ee,  0x81693da4,  0x1ce43052,  0x2e668418,  0x79e158c6,  0x4b63ec8c,
  0x07430d49,  0x35c1b903,  0x624665dd,  0x50c4d197,  0xcd49dc61,  0xffcb682b,  0xa84cb4f5,  0x9ace00bf,
  0x4827a958,  0x7aa51d12,  0x2d22c1cc,  0x1fa07586,  0x822d7870,  0xb0afcc3a,  0xe72810e4,  0xd5aaa4ae,
  0x69dff925,  0x5b5d4d6f,  0x0cda91b1,  0x3e5825fb,  0xa3d5280d,  0x91579c47,  0xc6d04099,  0xf452f4d3,
  0x26bb5d34,  0x1439e97e,  0x43be35a0,  0x713c81ea,  0xecb18c1c,  0xde333856,  0x89b4e488,  0xbb3650c2,
  0xf716b107,  0xc594054d,  0x9213d993,  0xa0916dd9,  0x3d1c602f,  0x0f9ed465,  0x581908bb,  0x6a9bbcf1,
  0xb8721516,  0x8af0a15c,  0xdd777d82,  0xeff5c9c8,  0x7278c43e,  0x40fa7074,  0x177dacaa,  0x25ff18e0,
  0x8f3c6f20,  0xbdbedb6a,  0xea3907b4,  0xd8bbb3fe,  0x4536be08,  0x77b40a42,  0x2033d69c,  0x12b162d6,
  0xc058cb31,  0xf2da7f7b,  0xa55da3a5,  0x97df17ef,  0x0a521a19,  0x38d0ae53,  0x6f57728d,  0x5dd5c6c7,
  0x11f52702,  0x23779348,  0x74f04f96,  0x4672fbdc,  0xdbfff62a,  0xe97d4260,  0xbefa9ebe,  0x8c782af4,
  0x5e918313,  0x6c133759,  0x3b94eb87,  0x09165fcd,  0x949b523b,  0xa619e671,  0xf19e3aaf,  0xc31c8ee5,
  0x520587f8,  0x608733b2,  0x3700ef6c,  0x05825b26,  0x980f56d0,  0xaa8de29a,  0xfd0a3e44,  0xcf888a0e,
  0x1d6123e9,  0x2fe397a3,  0x78644b7d,  0x4ae6ff37,  0xd76bf2c1,  0xe5e9468b,  0xb26e9a55,  0x80ec2e1f,
  0xcccccfda,  0xfe4e7b90,  0xa9c9a74e,  0x9b4b1304,  0x06c61ef2,  0x3444aab8,  0x63c37666,  0x5141c22c,
  0x83a86bcb,  0xb12adf81,  0xe6ad035f,  0xd42fb715,  0x49a2bae3,  0x7b200ea9,  0x2ca7d277,  0x1e25663d,
  0xb4e611fd,  0x8664a5b7,  0xd1e37969,  0xe361cd23,  0x7eecc0d5,  0x4c6e749f,  0x1be9a841,  0x296b1c0b,
  0xfb82b5ec,  0xc90001a6,  0x9e87dd78,  0xac056932,  0x318864c4,  0x030ad08e,  0x548d0c50,  0x660fb81a,
  0x2a2f59df,  0x18aded95,  0x4f2a314b,  0x7da88501,  0xe02588f7,  0xd2a73cbd,  0x8520e063,  0xb7a25429,
  0x654bfdce,  0x57c94984,  0x004e955a,  0x32cc2110,  0xaf412ce6,  0x9dc398ac,  0xca444472,  0xf8c6f038,
  0x44b3adb3,  0x763119f9,  0x21b6c527,  0x1334716d,  0x8eb97c9b,  0xbc3bc8d1,  0xebbc140f,  0xd93ea045,
  0x0bd709a2,  0x3955bde8,  0x6ed26136,  0x5c50d57c,  0xc1ddd88a,  0xf35f6cc0,  0xa4d8b01e,  0x965a0454,
  0xda7ae591,  0xe8f851db,  0xbf7f8d05,  0x8dfd394f,  0x107034b9,  0x22f280f3,  0x75755c2d,  0x47f7e867,
  0x951e4180,  0xa79cf5ca,  0xf01b2914,  0xc2999d5e,  0x5f1490a8,  0x6d9624e2,  0x3a11f83c,  0x08934c76,
  0xa2503bb6,  0x90d28ffc,  0xc7555322,  0xf5d7e768,  0x685aea9e,  0x5ad85ed4,  0x0d5f820a,  0x3fdd3640,
  0xed349fa7,  0xdfb62bed,  0x8831f733,  0xbab34379,  0x273e4e8f,  0x15bcfac5,  0x423b261b,  0x70b99251,
  0x3c997394,  0x0e1bc7de,  0x599c1b00,  0x6b1eaf4a,  0xf693a2bc,  0xc41116f6,  0x9396ca28,  0xa1147e62,
  0x73fdd785,  0x417f63cf,  0x16f8bf11,  0x247a0b5b,  0xb9f706ad,  0x8b75b2e7,  0xdcf26e39,  0xee70da73,
};

  for(i=0;i<256;i++)
      FastCRCTab[i] = CRCTab[i];


#if CRC_INIT_VAL == 0
  // Rolling CRC table construction
  for (i=0; i<256; i++)
  {
    unsigned crc = init_CRC();
    crc = update_CRC(crc,CRCTab,i);
    for (j=0; j<WINSIZE; j++)
      crc = update_CRC(crc,CRCTab,0);
    RollingCRCTab[i] = finish_CRC(crc);
//    printf("r-crc: %02x %08x %08x\n",i,r, (r & 1) ? (r>>1)^kCrcPoly : (r>>1));
  }

  // Check slow rolling CRC build.
  // BuildRollingCRCTable(FastCRCTab, SlowRollingCRCTab);
  // for (i=0; i<256; i++)
  //   if (RollingCRCTab[i] != SlowRollingCRCTab[i])   // unit testing :)
  //     printf("sr-crc: *%02x %08x %08x\n",i,RollingCRCTab[i],SlowRollingCRCTab[i]);

  // Fast table construction for rolling CRC
  // FastTableBuild (FastCRCTab, RollingCRCTab[128]);
  // for (i=0; i<256; i++)
  //   if (FastCRCTab[i] != RollingCRCTab[i])   // unit testing :)
  //     printf("fr-crc: %02x %08x %08x\n",i,FastCRCTab[i],RollingCRCTab[i]);

#else
  // BuildRollingCRCTable(FastCRCTab, RollingCRCTab);
#endif

  // for(i=0;i<256;i++) {
  //     printf(" %#010x, ", RollingCRCTab[i]);
  //     if(i % 8 == 7)
  //         printf("\n");
  // }

  // Example of rolling CRC calculation and unit test simultaneously
  unsigned char buffer[WINSIZE+TESTSIZE];
  for (i=0; i<WINSIZE+TESTSIZE; i++)
    buffer[i] = 11 + i*31 + i/17;   // random :)

  // Let's calc CRC(buffer+TESTSIZE,WINSIZE) in two ways
  crc1 = calcCRC(buffer+TESTSIZE,WINSIZE,CRCTab);
  crc2 = calcCRC(buffer,WINSIZE,CRCTab);
  printf("crc1: %08x\n", crc1);
  printf("crc2: %08x\n", crc2);
  crc2 = finish_CRC(crc2);
  for (i=0; i<TESTSIZE; i++)
  {
    crc2 = update_CRC(crc2,CRCTab,buffer[WINSIZE+i]) ^
      RollingCRCTab[buffer[i]];
  }
  crc2 = finish_CRC(crc2);
   printf("roll: %08x and %08x %s\n", crc1, crc2, crc1==crc2? "are equal":"ARE NOT EQUAL!");
  return 0;
}
