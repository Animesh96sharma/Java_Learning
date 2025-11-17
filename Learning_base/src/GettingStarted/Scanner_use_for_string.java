package GettingStarted;

import java.lang.*;
import java.util.*;

public class Scanner_use_for_string {
    static void main (String[] args) {

        String a;
        Scanner name = new Scanner(System.in);
        System.out.println("Can you tell me your name");
        a = name.nextLine();

        System.out.println("Welcome " + a + "!");
    }
}
