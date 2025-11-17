package GettingStarted;

import java.lang.*;
import java.util.*;

public class Scannerclass {
    public static void main(String[] args){
        Scanner s = new Scanner(System.in);
        int Number1, Number2, Sumvalue;
        System.out.println("Enter two integers");
        Number1= s.nextInt();
        Number2= s.nextInt();
        Sumvalue = Number1 + Number2;

        System.out.println("Sum of two numbers is- " + Sumvalue);

    }
}
