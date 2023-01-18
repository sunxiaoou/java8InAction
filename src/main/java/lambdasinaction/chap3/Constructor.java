package lambdasinaction.chap3;

import lambdasinaction.chap3.Lambdas.Apple;

import java.util.*;
import java.util.function.Function;

public class Constructor {
    public static void main(String ...args){
        List<Integer> weights = Arrays.asList(7, 3, 4, 10);
        List<Apple> apples = map(weights, Apple::new);
        System.out.println(apples);
    }

    public static List<Apple> map(List<Integer> list, Function<Integer, Apple> f){
        List<Apple> result = new ArrayList<>();
        for(Integer e: list){
            result.add(f.apply(e));
        }
        return result;
    }
}
