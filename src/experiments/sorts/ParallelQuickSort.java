package experiments.sorts;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/**
 * A parallelized quicksort class, currently for ArrayList<Integer> inputs.
 * Next phase will involve generics.
 *
 * Experiments in Fork/Join.
 * @Author Lauren Wolfe
 * @Date 2016/02/21
 * @Version 0.1
 */
public class ParallelQuickSort {
    //
    private static final int ARRAY_SIZE = 100;
    private static final int CUTOFF = 4;
    public static final int LEFT = -1;
    public static final int CENTER = 0;
    public static final int RIGHT = 1;
    protected static final ForkJoinPool POOL = new ForkJoinPool();

    public static void main(String[] args) {
        //Generate a pseudorandom ArrayList of values, then sort it.
        ArrayList<Integer> startArr = randIntArray(ARRAY_SIZE);
        ArrayList<Integer> sortedArr = doQuickSort(startArr);

        if(sortedArr == null || sortedArr.isEmpty()) {
            System.err.println("No result array passed back from parallel class");
            System.exit(1);
        } else {
            System.out.println("Input array:");
            System.out.println(startArr);
            System.out.println("Sorted array:");
            System.out.println(sortedArr.toString());
        }
    }

    /**
     * Sequential, recursive implementation of QuickSort
     * @param arr ArrayList of values to sort
     * @return sorted ArrayList
     */
    public static ArrayList<Integer> sequentialQuickSort(ArrayList<Integer> arr) {
        ArrayList<Integer> leftBranch, centerBranch, rightBranch;
        Integer parentArrSize = arr.size();

        //Base cases: handle 0/1 remaining values.
        if (parentArrSize == 0) {
            return null;
        } else if (parentArrSize == 1) {
            return arr;
        }

        //Split into new branches recursively.
        //Left < pivot, center == pivot, right > pivot
        leftBranch = sequentialQuickSort(splitArray(arr, LEFT));
        centerBranch = splitArray(arr, CENTER);
        if(parentArrSize == centerBranch.size()) {
            return centerBranch;
        }
        rightBranch = sequentialQuickSort(splitArray(arr, RIGHT));

        // Inorder Merge of result branches to form sorted ArrayList
        ArrayList<Integer> sortedArr = new ArrayList<>();
        if(leftBranch != null) {
            sortedArr.addAll(leftBranch);
        }
        sortedArr.addAll(centerBranch);
        if(rightBranch != null) {
            sortedArr.addAll(rightBranch);
        }
        return sortedArr;
    }

    /**
     * Using last indexed value as the pivot, split the existing array into three smaller arrays.
     * Left branch is lesser than pivot value, but otherwise unsorted. Center branch any copies of the pivot value.
     * Right branch is greater than pivot.
     * @param arr array portion to divide up
     * @param direction Which direction this split returns (LEFT, CENTER, RIGHT).
     * @return new branch ArrayList.
     */
    public static ArrayList<Integer> splitArray(ArrayList<Integer> arr, Integer direction) {
        if(direction == null) {
            throw new IllegalArgumentException("This function requires a direction input. To survive.");
        } else if(arr == null || arr.isEmpty()) {
            return arr;
        }

        ArrayList<Integer> childArr = new ArrayList<>();
        Integer pivot = getPivot(arr);

        //Selects from parent array based on direction parameter
        for(Integer val : arr) {
            if((direction == LEFT && val < pivot) || (direction == RIGHT && val > pivot) || (direction == CENTER && val == pivot)) {
                childArr.add(val);
            }
        }

        return childArr;
    }

    /**
     * Invokes the Fork/Join task.
     * @param arr current ArrayList subtask
     * @return sorted ArrayList.
     */
    public static ArrayList<Integer> doQuickSort(ArrayList<Integer> arr) {
        QuickSortTask task = new QuickSortTask(arr);
        return POOL.invoke(task);
    }

    /**
     * Parallelized quicksort task class.
     */
    private static class QuickSortTask extends RecursiveTask<ArrayList<Integer>> {
        ArrayList<Integer> arr, leftArr, centerArr, rightArr;

        /**
         * Constructor for Fork/Join task.
         * @param arr arraylist to sort.
         */
        protected QuickSortTask(ArrayList<Integer> arr) {
            this.arr = arr;
        }

        /**
         * Computes the divide and conquer and triggers array sorting
         * @return sorted array
         */
        public ArrayList<Integer> compute() {
            if(arr.size() < CUTOFF) {
                return sequentialQuickSort(arr);
            }

            //Create center branch containing any copies of pivot value.
            //If the array size isn't reduced, the remaining values are identical, so return.
            centerArr = splitArray(arr, CENTER);
            if(centerArr.size() == arr.size()) {
                return centerArr;
            }

            QuickSortTask leftTask = new QuickSortTask(splitArray(arr, LEFT));
            QuickSortTask rightTask = new QuickSortTask(splitArray(arr, RIGHT));

            rightTask.fork();
            leftArr = leftTask.compute();
            rightArr = rightTask.join();

            ArrayList<Integer> resultArr = new ArrayList<>();

            //Create the sorted ArrayList, adding branches from smallest to largest
            if(leftArr != null && !leftArr.isEmpty()) {
                resultArr.addAll(leftArr);
            }
            resultArr.addAll(centerArr);
            if(rightArr != null && !rightArr.isEmpty()) {
                resultArr.addAll(rightArr);
            }

            return resultArr;
        }
    }

    /**
     * Get rightmost value in current ArrayList.
     * @param arr arraylist containing values
     * @return the pivot value, which is used to sort the other array values into branches based on size.
     */
    public static Integer getPivot(ArrayList<Integer> arr) {
        if(arr == null || arr.isEmpty()) {
            throw new IllegalArgumentException();
        }
        return arr.get(Math.max(0, arr.size() - 1));
    }

    /**
     * Overriden pseudorandom ArrayList generator, allowing the user to specify a seed value,
     * for use in instantiating the Random object used in Integer generation.
     * @param size requested number of elements in resulting ArrayList
     * @return populated ArrayList of requested size
     */
    public static ArrayList<Integer> randIntArray(int size) {
        Random randoms = new Random();
        ArrayList<Integer> arr = new ArrayList<>();

        for(int i = 0; i < size; i++) {
            arr.add(randoms.nextInt(20));
        }
        return arr;
    }
}