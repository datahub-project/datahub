public class Josephus {
    public static int remaining(int number_of_people, int difference){
        if(number_of_people==1){
            return 0;
        }
        return (remaining(number_of_people-1,difference)+difference)%number_of_people;
    }
}