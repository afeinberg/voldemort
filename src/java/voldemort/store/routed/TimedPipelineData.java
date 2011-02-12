package voldemort.store.routed;

/**
 * Timed pipeline
 * @param <T>
 */
public class TimedPipelineData<T> extends BasicPipelineData<T> {

    private long startTimeNs;

    /**
      * Set start time to perform timeout correctly
      *
      * @param startTimeNs The start time in nanoseconds
      */
     public void setStartTimeNs(long startTimeNs) {
         this.startTimeNs = startTimeNs;
     }

     /**
      * Get start time to perform timeout correctly
      *
      * @return The start time in nanoseconds
      */
     public long getStartTimeNs() {
         return this.startTimeNs;
     }
}
