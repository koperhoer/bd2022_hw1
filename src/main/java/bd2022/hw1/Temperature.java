package bd2022.hw1;

// Класс температуры количества щелчков.
public class Temperature {
    // Текстовое обозначение температуры.
    public String temperature;
    // Начало диапазона.
    private long start;
    // Конец диапазона.
    private long end;
    // Метод говорит, лежит ли число в диапазоне этой температуры.
    public boolean contains(long temp) {
        if(temp >= start && temp < end)
            return true;
        return false;
    }
}
