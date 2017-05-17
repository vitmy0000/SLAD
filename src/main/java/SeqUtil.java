package com.weiz.slad;

public class SeqUtil {
  private static int wordSize;

  public static double kmerDist(SeqAsKmerCnt seq1, SeqAsKmerCnt seq2) {
    int i1 = 0;
    int i2 = 0;
    int[] indexArray1 = seq1.getIndexArray();
    int[] indexArray2 = seq2.getIndexArray();
    short[] countArray1 = seq1.getCountArray();
    short[] countArray2 = seq2.getCountArray();
    int intersectCnt = 0;
    while (i1 < indexArray1.length && i2 < indexArray2.length) {
      if (indexArray1[i1] < indexArray2[i2]) {
        i1++;
      } else if (indexArray1[i1] > indexArray2[i2]) {
        i2++;
      } else { // indexArray1[i1] == indexArray2[i2]
        intersectCnt += Math.min(countArray1[i1], countArray2[i2]);
        i1++;
        i2++;
      }
    }
    return 1 - (double)intersectCnt /
    (Math.min(seq1.getRead().length(), seq2.getRead().length()) - seq1.getWordSize() + 1);
  }

  public static double nwDist(String seq1, String seq2) {
    int len1 = seq1.length();
    int len2 = seq2.length();
    // score of best alignment of x[1..i] and y[1..j] ending with a char-char match or mismatch
    int[][] M = new int[len1 + 1][len2 + 1];
    // score of best alignment of x[1..i] and y[1..j] ending with a space in x
    int[][] X = new int[len1 + 1][len2 + 1];
    // score of best alignment of x[1..i] and y[1..j] ending with a space in y
    int[][] Y = new int[len1 + 1][len2 + 1];
    // direction matrix for backtracking
    char[][] dirM = new char[len1 + 1][len2 + 1];
    char[][] dirX = new char[len1 + 1][len2 + 1];
    char[][] dirY = new char[len1 + 1][len2 + 1];

    int matchScore = 5;
    int missScore = -4;
    int gapOpenPenalty = 10;
    int gapExtendPenalty = 1;

    for (int i = 1; i < len1; i++) {
      M[i][0] = -999999999;
      X[i][0] = -999999999;
      Y[i][0] = -gapOpenPenalty - i * gapExtendPenalty;
    }
    for (int j = 1; j < len2; j++) {
      M[0][j] = -999999999;
      X[0][j] = -gapOpenPenalty - j * gapExtendPenalty;
      Y[0][j] = -999999999;
    }

    for (int i = 1; i <= len1; i++) {
      for (int j = 1; j <= len2; j++) {
        int addScore = 0;
        if (seq1.charAt(i - 1) == seq2.charAt(j - 1)) {
          addScore = matchScore;
        } else {
          addScore = missScore;
        }
        // M
        int scoreMM = M[i - 1][j - 1] + addScore;
        int scoreMX = X[i - 1][j - 1] + addScore;
        int scoreMY = Y[i - 1][j - 1] + addScore;
        if (scoreMM >= scoreMX && scoreMM >= scoreMY) {
          M[i][j] = scoreMM;
          dirM[i][j] = 'M';
        } else if (scoreMX >= scoreMM && scoreMX >= scoreMY) {
          M[i][j] = scoreMX;
          dirM[i][j] = 'X';
        } else if (scoreMY >= scoreMM && scoreMY >= scoreMX) {
          M[i][j] = scoreMY;
          dirM[i][j] = 'Y';
        }
        // X
        int scoreXM = M[i][j - 1] - gapOpenPenalty - gapExtendPenalty;
        int scoreXX = X[i][j - 1] - gapExtendPenalty;
        int scoreXY = Y[i][j - 1] - gapOpenPenalty - gapExtendPenalty;
        if (scoreXM >= scoreXX && scoreXM >= scoreXY) {
          X[i][j] = scoreXM;
          dirX[i][j] = 'M';
        } else if (scoreXX >= scoreXM && scoreXX >= scoreXY) {
          X[i][j] = scoreXX;
          dirX[i][j] = 'X';
        } else if (scoreXY >= scoreXM && scoreXY >= scoreXX) {
          X[i][j] = scoreXY;
          dirX[i][j] = 'Y';
        }
        // Y
        int scoreYM = M[i - 1][j] - gapOpenPenalty - gapExtendPenalty;
        int scoreYX = X[i - 1][j] - gapOpenPenalty - gapExtendPenalty;
        int scoreYY = Y[i - 1][j] - gapExtendPenalty;
        if (scoreYM >= scoreYX && scoreYM >= scoreYY) {
          Y[i][j] = scoreYM;
          dirY[i][j] = 'M';
        } else if (scoreYX >= scoreYM && scoreYX >= scoreYY) {
          Y[i][j] = scoreYX;
          dirY[i][j] = 'X';
        } else if (scoreYY >= scoreYM && scoreYY >= scoreYX) {
          Y[i][j] = scoreYY;
          dirY[i][j] = 'Y';
        }
      }
    }

    //backtracking
    int i = len1;
    int j = len2;
    int missCnt = 0;
    int matchCnt = 0;
    char[][] dir;
     if (X[i][j] >= M[i][j] && X[i][j] >= Y[i][j]) {
      dir = dirX;
    } else if (Y[i][j] >= M[i][j] && Y[i][j] >= X[i][j]) {
      dir = dirY;
    } else {
      dir = dirM;
    }
    while (i > 0 && j > 0) {
      char dirChar = dir[i][j];
      if (dir == dirM) {
        if (seq1.charAt(i - 1) == seq2.charAt(j - 1)) {
          matchCnt++;
        } else {
          missCnt++;
        }
        i--;
        j--;
      } else if (dir == dirX) {
        missCnt++;
        j--;
      } else if (dir == dirY) {
        missCnt++;
        i--;
      }
      if (dirChar == 'M') {
        dir = dirM;
      } else if (dirChar == 'X') {
        dir = dirX;
      } else if (dirChar == 'Y') {
        dir = dirY;
      }
    }
    missCnt += i;
    missCnt += j;

    return (double)missCnt / (missCnt + matchCnt);
  }
}
