package GettingStarted;

import java.lang.*;
import java.util.*;

public class Firstcode {
     static void main(String[] args){
        System.out.println("This is my first code");
        System.out.println("Enter your name");
        Scanner name = new Scanner(System.in);
        String name1;
        name1 = name.next();
        System.out.println(name1);
        System.out.println("Welcome " + name1);
    }
}