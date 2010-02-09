package voldemort.client.rebalance;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.serialization.json.JsonReader;
import voldemort.serialization.json.JsonWriter;

import com.google.common.collect.ImmutableMap;

public class RebalancePartitionsInfo {

    private final int stealerId;
    private final int donorId;
    private final List<Integer> partitionList;
    private final List<Integer> deletePartitionsList;
    private List<String> unbalancedStoreList;

    private int attempt;

    /**
     * Rebalance Partitions info maintains all information needed for
     * rebalancing of one stealer node from one donor node.
     * <p>
     * 
     * @param stealerNodeId
     * @param donorId
     * @param partitionList
     * @param deletePartitionsList : For cases where replication mapping is
     *        changing due to partition migration we only want to copy data and
     *        not delete them from donor node.
     * @param unbalancedStoreList
     * @param attempt
     */
    public RebalancePartitionsInfo(int stealerNodeId,
                                   int donorId,
                                   List<Integer> partitionList,
                                   List<Integer> deletePartitionsList,
                                   List<String> unbalancedStoreList,
                                   int attempt) {
        super();
        this.stealerId = stealerNodeId;
        this.donorId = donorId;
        this.partitionList = partitionList;
        this.attempt = attempt;
        this.deletePartitionsList = deletePartitionsList;
        this.unbalancedStoreList = unbalancedStoreList;
    }

    @SuppressWarnings("unchecked")
    public static RebalancePartitionsInfo fromString(String line) {
        try {
            JsonReader reader = new JsonReader(new StringReader(line));
            Map<String, Object> map = (Map<String, Object>) reader.read();
            return fromMap(map);
        } catch(Exception e) {
            throw new VoldemortException("Failed to create RebalanceStealInfo from String:" + line,
                                         e);
        }
    }

    public static List<RebalancePartitionsInfo> listFromString(String line) {
        try {
            List<RebalancePartitionsInfo> rebalancePartitionsInfoList = new ArrayList<RebalancePartitionsInfo>();
            JsonReader reader = new JsonReader(new StringReader(line));
            for (Object o: reader.readArray()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) o;
                rebalancePartitionsInfoList.add(fromMap(map));
            }
            return rebalancePartitionsInfoList;
        } catch (Exception e) {
            throw new VoldemortException("Failed to create a RebalanceStealInfo List from String: " + line,
                                         e);
        }
    }

    @SuppressWarnings("unchecked")
    protected static RebalancePartitionsInfo fromMap(Map<String, Object> map) {
        int stealerId = (Integer) map.get("stealerId");
        int donorId = (Integer) map.get("donorId");
        List<Integer> partitionList = (List<Integer>) map.get("partitionList");
        int attempt = (Integer) map.get("attempt");
        List<Integer> deletePartitionsList = (List<Integer>) map.get("deletePartitionsList");
        List<String> unbalancedStoreList = (List<String>) map.get("unbalancedStoreList");
        return new RebalancePartitionsInfo(stealerId,
                                            donorId,
                                            partitionList,
                                            deletePartitionsList,
                                            unbalancedStoreList,
                                            attempt);
    }

    public List<Integer> getDeletePartitionsList() {
        return deletePartitionsList;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public int getDonorId() {
        return donorId;
    }

    public List<Integer> getPartitionList() {
        return partitionList;
    }

    public int getAttempt() {
        return attempt;
    }

    public int getStealerId() {
        return stealerId;
    }

    public List<String> getUnbalancedStoreList() {
        return unbalancedStoreList;
    }

    public void setUnbalancedStoreList(List<String> storeList) {
        this.unbalancedStoreList = storeList;
    }

    @Override
    public String toString() {
        return "RebalancingStealInfo(" + getStealerId() + " <--- " + getDonorId() + " partitions:"
               + getPartitionList() + " stores:" + getUnbalancedStoreList() + ")";
    }

    @SuppressWarnings("unchecked")
    public String toJsonString() {
        Map map = toMap();

        StringWriter writer = new StringWriter();
        new JsonWriter(writer).write(map);
        writer.flush();
        return writer.toString();
    }

    public ImmutableMap<Object, Object> toMap() {
        return ImmutableMap.builder()
                           .put("stealerId", stealerId)
                           .put("donorId", donorId)
                           .put("partitionList", partitionList)
                           .put("unbalancedStoreList", unbalancedStoreList)
                           .put("deletePartitionsList", deletePartitionsList)
                           .put("attempt", attempt)
                           .build();
    }

    @SuppressWarnings("unchecked")
    public static String listToJsonString(List<RebalancePartitionsInfo> rebalancePartitionsInfoList) {
        List maps = new ArrayList();
        for (RebalancePartitionsInfo rebalancePartitionsInfo: rebalancePartitionsInfoList) {
            maps.add(rebalancePartitionsInfo.toMap());
        }

        StringWriter writer = new StringWriter();
        new JsonWriter(writer).write(maps);
        writer.flush();
        return writer.toString();
    }


}