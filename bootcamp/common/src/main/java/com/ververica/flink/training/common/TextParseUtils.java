package com.ververica.flink.training.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextParseUtils {

    public static String getTextField(String s, String fieldName) {
        Pattern p = Pattern.compile(String.format("%s='([^']*)'", fieldName));
        Matcher m = p.matcher(s);
        if (m.find()) {
            return m.group(1);
        }

        throw new RuntimeException("Can't field " + fieldName + " in " + s);
    }

    public static String getField(String s, String fieldName) {
        Pattern p = Pattern.compile(String.format("%s=(.+?)(, |\\}|$)", fieldName));
        Matcher m = p.matcher(s);
        if (m.find()) {
            return m.group(1);
        }

        throw new RuntimeException("Can't field " + fieldName + " in " + s);
    }

}
