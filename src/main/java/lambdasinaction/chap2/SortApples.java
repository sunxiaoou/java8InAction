package lambdasinaction.chap2;

import lambdasinaction.chap2.FilteringApples.Apple;

import java.util.Arrays;
import java.util.List;

public class SortApples {

    public static void main(String ... args){
        List<FilteringApples.Apple> inventory =
                Arrays.asList(new Apple(80,"green"),
                        new Apple(155, "green"),
                        new Apple(120, "red"));
        inventory.sort((Apple a1, Apple a2) -> a1.getWeight().compareTo(a2.getWeight()));
        System.out.println(inventory);
        inventory.sort((Apple a1, Apple a2) -> a1.getColor().compareTo(a2.getColor()));
        System.out.println(inventory);
    }
}
