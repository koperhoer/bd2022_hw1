package bd2022.hw1;

// Класс элемента экрана.
public class ScreenElement {
    // Тип: кнопка, рекламный баннер, элемент навигации, декоративный элемент, поля для ввода.
    private String type;
    // Логическое имя элемента.
    private String logic_name;
    // Координаты X и Y начала элемента. Полагается, что верхний левый угол
    // экрана - точка (0,0), нижний правый - (1920,1080).
    private int startX;
    private int startY;
    // Координаты X и Y конца элемента. Полагается, что они больше или равны
    // координатам начала.
    private int endX;
    private int endY;

    // Преобразование в текст.
    @Override
    public String toString() {
        return "Type: " + this.type + ", logic name: " + this.logic_name + ", X coords = " + Integer.toString(startX) + "," + Integer.toString(endX) +
                ", Y coords = " + Integer.toString(startY) + "," + Integer.toString(endY);
    }
    // Метод говорит, попадает ли нажатие на некоторые координаты в экране на данный элемент.
    public boolean containsClick(Click click) {
        return (click.X >= this.startX && click.X <= this.endX && click.Y >= this.startY && click.Y <= this.endY);
    }
    // Метод-геттер логического имени элемента.
    public String getLogicName() {
        return logic_name;
    }
}
