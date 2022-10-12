package oops;

import java.util.ArrayList;
class producer implements Runnable{
    ArrayList<Integer> queue=new ArrayList<>();
    int size;
    producer(ArrayList<Integer> queue,int size){
        this.queue=queue;
        this.size=size;
    }
    @Override
    public void run() {
        for(int i=0;i<7;i++){
            if(queue.size()==size){
                synchronized (queue){
                    System.out.println("queue is full producer is waiting");
                    try{
                        queue.wait();
                    }
                    catch (InterruptedException e){
                        System.out.println("Interupted");
                    }
                }
            }
            synchronized (queue)
            {
                System.out.println("produced"+i+queue.add(i));
                queue.notifyAll();
            }
        }
    }
}

class consumer implements Runnable{
    ArrayList<Integer> queue;
    final int size;
    boolean flag=true;
    public consumer(ArrayList<Integer> queue, int size) {
        this.queue=queue;
        this.size=size;
    }
    @Override
    public void run() {
        while(true){
            while(queue.isEmpty())
            {
                synchronized(queue){
                    try {
                        queue.wait();
                        System.out.println("out of waiting");

                    } catch (InterruptedException ex) {
                        System.out.println("interrupted");
                    }
                }
            }
            synchronized(queue)
            {
                queue.notifyAll();
                int k=queue.remove(0);
                System.out.println("consumed " + k);
            }
        }} }
public class Consumer_Producer {
    public static void main(String[] args) throws InterruptedException {
        ArrayList<Integer> queue=new ArrayList<>();
        int size=5;
        consumer c=new consumer(queue,size);
        producer p=new producer(queue,size);
        Thread pthread=new Thread(p,"producer");
        Thread cthread=new Thread(c,"consumer");

        pthread.start();
        cthread.start();
    }
}
