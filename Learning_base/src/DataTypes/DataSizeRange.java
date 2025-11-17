package DataTypes;

import java.lang.*;
import java.util.Scanner;

public class DataSizeRange {

    static void main(String[] args){
        System.out.println(Byte.MIN_VALUE);
        System.out.println(Byte.MAX_VALUE);
        System.out.println(Byte.BYTES);
        byte a = (byte) 129;
        System.out.println(a);
        int x, X;
        int sum;
        System.out.println("Enter the value of x and X");
        Scanner SC = new Scanner(System.in);
        x = SC.nextInt();
        X = SC.nextInt();
        sum = x + X;
        System.out.println("Sum of two numbers is " + sum);
    }
}

