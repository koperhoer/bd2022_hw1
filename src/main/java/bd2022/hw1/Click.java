package bd2022.hw1;

// Класс, хранящий информацию о нажатиях на экран.
public class Click {
    // Координаты X и Y точки нажатия.
    public int X;
    public int Y;

    // Конструируем класс из строки. В сущности, ID пользователя и
    // метка времени в нашей задаче не нужны вообще. Поэтому достаточно
    // проверить, в нужном ли формате строка, и использовать только координаты.
    public Click(String click_str) throws Exception {
        String[] click_arr = click_str.split(",");
        // Строка состоит из координат X и Y нажатия, UID, метки времени.
        // Через запятую без пробелов.
        if(click_arr.length != 4)
            throw new Exception("Click initialiser string is incorrect.");
        try {
            X = Integer.parseInt(click_arr[0]);
            Y = Integer.parseInt(click_arr[1]);
        }
        catch (Exception e) {
            // Исключительные случаи, когда строка не по формату, отлавливаем.
            throw new RuntimeException(e);
        }
    }
    // Текстовая интерпретация нажатия использовалась при ручном тестировании.
    @Override
    public String toString() {
        return "Click: X = " + Integer.toString(X) + ", Y = " + Integer.toString(Y);
    }
}
