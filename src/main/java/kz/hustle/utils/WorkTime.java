package kz.hustle.utils;

public class WorkTime {
    private static WorkTime ourInstance = null;
    private long startWorkTime = 0;

    public static WorkTime get() {
        if (ourInstance == null) {
            synchronized (WorkTime.class) {
                if (ourInstance == null) {
                    ourInstance = new WorkTime();
                }
            }
        }
        return ourInstance;
    }

    public void startTime() {
        startWorkTime = System.currentTimeMillis();
    }

    public String getWorkTimeShort() {
        long diff = System.currentTimeMillis() - startWorkTime;
        long diffSeconds = diff / 1000 % 60;
        long diffMinutes = diff / (60 * 1000) % 60;
        long diffHours = diff / (60 * 60 * 1000) % 24;

        return String.format("%02d:%02d:%02d", diffHours, diffMinutes, diffSeconds);
    }
}
