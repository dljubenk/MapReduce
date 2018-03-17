import java.io.*;
import java.util.*;

public class MyMapReduce
{
	// stvaranje novih listi
	List buckets = new ArrayList();
	List intermediateresults = new ArrayList();
	List values = new ArrayList();

	public void init()
	{	
		// u listu values dodaj imena dokumenata	
		// --> nadodati vlastiti kod
		values.add("doc1.txt");
		values.add("doc2.txt");

		System.out.println("**STEP 1 START**-> Running Conversion into Buckets**");
		System.out.println();
		List b = step1ConvertIntoBuckets(values, 2);
        	System.out.println("************STEP 1 COMPLETE*************");
        	System.out.println();
        	System.out.println();

   		System.out.println("**STEP 2 START**->Running **Map Function** concurrently for all Buckets");
		System.out.println();
		List res = step2RunMapFunctionForAllBuckets(b);
		System.out.println("************STEP 2 COMPLETE*************");

        	System.out.println();
        	System.out.println();
		System.out.println("**STEP 3 START**->Running **Reduce Function** for collating Intermediate Results and Printing Results");
		System.out.println();
		step3RunReduceFunctionForAllBuckets(res);
		System.out.println("************STEP 3 COMPLETE*************");

	}

	// Funkcija: dijeli imena primljenih dokumenata u buckete
	// Prima: listu u kojoj se nalaze imena dokumenata koje je potrebno podijeliti u buckete, te broj bucketa (2)
	// Vraca: ispunjene buckete
	public List step1ConvertIntoBuckets(List list, int numberofbuckets)
	{
		int n = list.size(); // 2 (jer imamo dva dokumenta)
		int m = n / numberofbuckets; // 2/2 = 1 --> u svaki bucket trebamo staviti po jedan dokument
		int rem = n % numberofbuckets; // 2%2 = 0 --> nema ostatka, pa ce nam svi dokumenti stati u predvidjene buckete (tj. ne treba nam dodatni bucket)

		int count = 0;
		System.out.println("BUCKETS");

		for (int j = 1; j <= numberofbuckets; j++)
		{
			List temp = new ArrayList();
			for (int i = 1; i <= m; i++)
			{
				// dohvati ime prvog dokumenta i spremi ga u temp
				// --> nadodati vlastiti kod
				temp.add((String)values.get(count));

				count++;
			}

			// u buckets ubaci ime prvog dokumenta, pa drugog
			// --> nadodati vlastiti kod
			buckets.add(temp);

			temp = new ArrayList();
		}

		if (rem != 0) // ako ne mozemo podatke jednoliko podijeliti u buckete, ostatak stavi u novi bucket
		{
			List temp = new ArrayList();

			for (int i = 1; i <= rem; i++)
			{
				temp.add((String)values.get(count));
				count++;
			}

			buckets.add(temp);
		}

       		System.out.println();
		System.out.println(buckets);
		System.out.println();
		return buckets;
	}

	// Funkcija: Broji koliko ima odredjenih rijeci u svakom bucketu (npr. rijec FESB se u 1. bucketu pojavljuje 12 puta, rijec SPLIT 5, itd.)
	// Prima: listu bucketa (tj. imena dvaju dokumenata koja su podijeljena u 2 bucketa)
	// Vraca: hash tablice za svaki dokument/bucket (u njima pise koliko se puta pojavljuje koja rijec)
	public List step2RunMapFunctionForAllBuckets(List list)
	{
		for (int i = 0; i < list.size(); i++) // za svaki bucket
		{
			List elementList = (ArrayList)list.get(i);

			// kreiranje nove instance klase StartThread i pozivanje start metode
			// --> nadodati vlastiti kod
			new StartThread(elementList).start();
		}

        	try
        	{
			Thread.currentThread().sleep(1000); // cekamo dok se sve ne sinkronizira (za svaki slucaj)
		}
		catch(Exception e)
		{
		}

		return intermediateresults; // tu su hash tablice za svaki dokument/bucket
	}

	// ne prikazuj upozorenja, programer je zelio ovakav kod
	@SuppressWarnings("unchecked")

	// Funkcija: kombinira rezultate hash funkcija za sve buckete (ukratko, zbraja koliko se puta odredjene rijeci ponavljaju u svim bucketima)
	// Prima: hash tablice za svaki dokument/bucket
	// Vraca: nista
	public void step3RunReduceFunctionForAllBuckets(List list)
	{
		int sum = 0;
		
		Hashtable <String,Integer > wordMap = new Hashtable <String,Integer>(); // stvori hash tablicu u kojoj ce biti ukupni rezultati
		wordMap.putAll((Hashtable <String,Integer >) list.get(0)); // u wordMap stavi prvu hash tablicu jer se ona nema s cime duplicirati (sve rijeci u njoj su jedinstvene)

		for (int i = 1; i < list.size(); i++) // za sve ostale buckete
		{
			Hashtable <String,Integer > newMap = (Hashtable <String,Integer >) list.get(i); // dohvati drugu hash tablicu
			for (String key : newMap.keySet()) // za svaki key iz keyset-a kojeg dohvatimo iz druge hash tablice
			{
				if (wordMap.containsKey(key)) // ako ukupna hash tablica sadrzi taj key
				{
					int val = wordMap.get(key).intValue() + newMap.get(key).intValue(); // zbroji broj ponavljanja te rijeci
					wordMap.put(key, val); // update-aj ukupnu hash tablicu
				}
				else // ako ukupna hash tablica ne sadrzi taj key
				{
					wordMap.put(key, newMap.get(key)); // dodaj novu rijec u ukupnu hash tablicu
				}
			}

			System.out.println();
			System.out.println("Konacni rezultati: ");

			for (String key : wordMap.keySet())
			{
				System.out.println(key + ":" + wordMap.get(key));
			}
		}
		
		System.out.println();
	}

	// Funkcija: kreira hash tablice za svaki bucket
	// 	     u svakoj hash tablici nalaze se rijeci iz bucketa, te broj puta koliko se ponavljaju u tom bucketu
	// Update se globalna lista intermediateresults u kojoj su hash tablice za sve buckete
	class StartThread extends Thread
	{
		private List tempList = new ArrayList();
		int brojr; // broj rijeci

		public StartThread(List list) // dobije ime (prvog, pa drugog) dokumenta; ovo se pozove kod kreiranja instance klase
		{
			tempList = list;
			System.out.println("Thread " + tempList);
		}

		public void run() // metoda start poziva run funkciju
		{	
			brojr = 0;

			// Hashtable (Hashtable <String,Integer>) mapira keys u values
			// --> nadodati vlastiti kod (kreirajte hash tablicu imena wordMap)
			Hashtable <String,Integer> wordMap = new Hashtable <String,Integer>();
  
			for (int i = 0; i < tempList.size(); i++) // za sve elemente jednog bucketa (petlja nam treba za slucaj da u bucketu imamo vise dokumenata)
			{
				// ucitavanje dokumenata
				File inFile = new File((String)tempList.get(i));
				try
				{  
					BufferedReader br = new BufferedReader(new FileReader(inFile));
						
					// ready() metoda nam kaze jesu li podaci dostupni (npr. je li ih server preko soketa vec poslao)
					while (br.ready())
					{
						// definicija: StringTokenizer(String str, String delimiters)
						// delimiteri se ne bi trebali vracati kao tokeni - njih moramo zanemariti prilikom parsiranja teksta
						// --> nadodati vlastiti kod ( StringTokenizer line = new StringTokenizer(--> nadopisite sto dolazi unutar ovih zagrada <--); )
						StringTokenizer line = new StringTokenizer (br.readLine(), " ,.;:()");
						
						while (line.hasMoreTokens())
						{
							String temp = line.nextToken(); // ucitaj slijedeci toket (tj. rijec)
							brojr++;
							
							// ako se u hash tablici vec nalazi ova rijec
							if (wordMap.containsKey(temp))
							{
								int val = wordMap.get(temp).intValue();
								val++;
								wordMap.put(temp, val);
								// --> nadodati vlastiti kod. Smjernice:
								// dohvati tu rijec iz hash tablice, te broj puta koliko se je ponovila
								// uvecaj broj puta koliko se je ta rijec ponovila
								// update-aj podatke u hash tablici
							}
							else // rijec ne postoji u hash tablici, pa je trebamo nadodati
							{
								wordMap.put(temp, 1);
								// --> nadodati vlastiti kod
							}		
						}
					}
				}
				catch (Exception e)
				{
					// u slucaju greske, ispisi slijed funkcija koje su rezultirale pozivanjem funkcije koja je generirala gresku
					e.printStackTrace();
				}

				String str = (String)tempList.get(i);

				// kriticni odsjecak
				synchronized(this)
                     		{
					// u listu intermediateresults dodaj hash tablicu za trenutni bucket
					// --> nadodati vlastiti kod
					intermediateresults.add(wordMap);
				}
			}
		}
	}
}

