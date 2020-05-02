package kafka.examples.producer;

public class Util {
    private static final String sep = "\036";

    public static String getNthColumn(String line, int nth) {
        return getNthColumn(line, nth, sep);
    }

    public static String getNthColumn(String line, int nth, String rs) {
        if(nth < 1) {
            return "";
        }

        String[] tokens = line.split(rs);
        if(tokens.length < nth) {
            return "";
        }
        return tokens[nth-1];
    }

    public static String getFirstColumn(String line) {
        return line.substring(0, line.indexOf(sep));
    }
}
