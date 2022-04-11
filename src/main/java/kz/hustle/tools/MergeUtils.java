package kz.hustle.tools;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class MergeUtils {
    public static String getWorkTime(long workTime) {
        long diffSeconds = workTime / 1000 % 60;
        long diffMinutes = workTime / (60 * 1000) % 60;
        long diffHours = workTime / (60 * 60 * 1000) % 24;

        return String.format("%02d:%02d:%02d", diffHours, diffMinutes, diffSeconds);
    }
}
