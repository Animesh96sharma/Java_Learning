package GettingStarted;

import java.util.Scanner;
import java.lang.*;
import java.util.*;

public class useRadix {
    static void main (String[] args) {

            int a;
            Scanner name = new Scanner(System.in);
            System.out.println("Give me a number");
            name.useRadix(2);
            a = name.nextInt();

            System.out.println(a);
        }
    }
//use for binary number
