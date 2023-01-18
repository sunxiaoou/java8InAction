package lambdasinaction.chap5;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public  class Trader{
	
	private String name;
	private String city;

	public Trader(String n, String c){
		this.name = n;
		this.city = c;
	}

	public String getName(){
		return this.name;
	}

	public String getCity(){
		return this.city;
	}

	public void setCity(String newCity){
		this.city = newCity;
	}

	public String toString(){
		return "Trader:"+this.name + " in " + this.city;
	}

	public static void main(String... args) {
		Trader raoul = new Trader("Raoul", "Cambridge");
		Trader mario = new Trader("Mario","Milan");
		Trader alan = new Trader("Alan","Cambridge");
		Trader brian = new Trader("Brian","Cambridge");

		List<Transaction> transactions = Arrays.asList(
				new Transaction(brian, 2011, 300),
				new Transaction(raoul, 2012, 1000),
				new Transaction(raoul, 2011, 400),
				new Transaction(mario, 2012, 710),
				new Transaction(mario, 2012, 700),
				new Transaction(alan, 2012, 950));

		List<Transaction> t2011 = transactions.stream().filter(x -> x.getYear() == 2011)
				.sorted(comparing(Transaction::getValue))
				.collect(toList());
		System.out.println(t2011);		// 1

		List<Trader> traders = transactions.stream().map(Transaction::getTrader)
				.distinct()
				.collect(toList());
		// System.out.println(traders);

		List<String> cities = traders.stream().map(Trader::getCity)
				.distinct()
				.collect(toList());
		System.out.println(cities);		// 2

		List<Trader> traders2 = traders.stream().filter(x -> x.getCity().equals("Cambridge"))
				.sorted(comparing(Trader::getName))
				.collect(toList());
		System.out.println(traders2);	// 3

		String nameStr = traders.stream().map(Trader::getName).sorted().reduce("", (x1, x2) -> (x1 + x2));
		System.out.println(nameStr);	// 4

		boolean any = traders.stream().anyMatch(x -> x.getCity().equals("Milan"));
		System.out.println(any);		// 5

		transactions.stream().filter(x -> x.getTrader().getCity().equals("Cambridge"))
				.map(Transaction::getValue)
				.forEach(System.out::println);	// 6

		Optional<Integer> max = transactions.stream().map(Transaction::getValue).reduce(Integer::max);
		max.ifPresent(System.out::println);

//		Optional<Transaction> minTr = transactions.stream()
//				.reduce((x1, x2) -> x1.getValue() < x2.getValue() ? x1 : x2);
		Optional<Transaction> minTr = transactions.stream().min(comparing(Transaction::getValue));
		minTr.ifPresent(System.out::println);
	}
}