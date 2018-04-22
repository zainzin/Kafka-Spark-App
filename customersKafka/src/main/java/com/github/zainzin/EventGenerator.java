package com.github.zainzin;

import com.github.javafaker.Address;
import com.github.javafaker.Faker;
import com.github.javafaker.Name;
import com.github.javafaker.PhoneNumber;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

public class EventGenerator {

    static Name generateFullName() {
        Faker faker = new Faker();
        return faker.name();
    }

    static String generateEmail(String name) {
        String[] domains = new String[]{"@gmail.com", "@yahoo.com", "@msn.com", "@live.com", "@hotmail.com"};
        Random random = new Random();
        return name.concat(domains[random.nextInt(domains.length)]);
    }

    public static Address generateAddress() {
        Faker faker = new Faker();
        return faker.address();
    }

    static PhoneNumber generatePhoneNumber() {
        Faker faker = new Faker();
        return faker.phoneNumber();
    }

    static String generateIp() {
        Random random = new Random();
        return String.valueOf(random.nextInt(255) + 1) + "."
                + String.valueOf(random.nextInt(255) + 1) + "." +
                String.valueOf(random.nextInt(255) + 1) + "." +
                String.valueOf(random.nextInt(255) + 1);
    }

    static String generateGender() {
        String[] gender = new String[]{"Male", "Female"};
        Random random = new Random();
        return gender[random.nextInt(1)];
    }

    static String generateDate(int startYear, int endYear) {
        GregorianCalendar gc = new GregorianCalendar();
        int year = randBetween(startYear, endYear);
        gc.set(Calendar.YEAR, year);
        int dayOfYear = randBetween(1, gc.getActualMaximum(Calendar.DAY_OF_YEAR));
        gc.set(Calendar.DAY_OF_YEAR, dayOfYear);
        return gc.get(Calendar.YEAR) + "/" + (gc.get(Calendar.MONTH) + 1) + "/" + gc.get(Calendar.DAY_OF_MONTH);
    }

    static int randBetween(int start, int end) {
        return start + (int) Math.round(Math.random() * (end - start));
    }
}
