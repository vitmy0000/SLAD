package com.weiz.slad;

import java.io.Serializable;

public class SeqAsKmerCnt implements Serializable {

  private String read;
  private int wordSize;
  private int[] indexArray;
  private short[] countArray;

  // TODO: add ambiguous letters
  private static int[] charMap = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  };

  public SeqAsKmerCnt(String read, int wordSize) {
    this.read = read;
    this.wordSize = wordSize;
    short[] fullSizeArray = new short[2 << (2 * wordSize)];
    int index = 0;
    for (int i = 0; i < wordSize - 1; i++) {
      index <<= 2;
      index |= charMap[read.charAt(i)];
    }
    int mask = (1 << 2 * wordSize) - 1;
    int compactSize = 0;
    for (int i = 0; i < read.length() - wordSize + 1; i++) {
      index <<= 2;
      index |= charMap[read.charAt(wordSize - 1 + i)];
      index &= mask;

      if (fullSizeArray[index] == 0) {
        compactSize++;
      }
      fullSizeArray[index]++;
    }
    // fetch non-zero enties only
    indexArray = new int[compactSize];
    countArray = new short[compactSize];
    int increment = 0;
    for (int i = 0; i < fullSizeArray.length; i++) {
      if (fullSizeArray[i] != 0) {
        indexArray[increment] = i;
        countArray[increment] = fullSizeArray[i];
        increment++;
      }
    }
  }

  public String getRead()  {
    return read;
  }

  public int getWordSize()  {
    return wordSize;
  }

  public int[] getIndexArray() {
    return indexArray;
  }

  public short[] getCountArray() {
    return countArray;
  }
}
