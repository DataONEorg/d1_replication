package org.dataone.service.cn.replication;

public class ApiVersion implements Comparable<ApiVersion> {

    private String apiLabel = "";
    private int apiValue = 0;

    private static final String VERSION_ONE_STRING = "v1";
    private static final String VERSION_TWO_STRING = "V2";

    public static final ApiVersion V1 = new ApiVersion(VERSION_ONE_STRING, 1);
    public static final ApiVersion V2 = new ApiVersion(VERSION_TWO_STRING, 2);

    private ApiVersion(String label, int value) {
        this.apiLabel = label;
        this.apiValue = value;
    }

    private ApiVersion() {
    }

    public static ApiVersion getVersion(String label) {
        if (V1.getApiLabel().equals(label)) {
            return V1;
        } else if (V2.getApiLabel().equals(label)) {
            return V2;
        } else {
            return null;
        }
    }

    public static ApiVersion getV1Version() {
        return V1;
    }

    public static ApiVersion getV2Version() {
        return V2;
    }

    public static boolean isV1(String label) {
        if (VERSION_ONE_STRING.equals(label)) {
            return true;
        }
        return false;
    }

    public static boolean isV2(String label) {
        if (VERSION_TWO_STRING.equals(label)) {
            return true;
        }
        return false;
    }

    public String getApiLabel() {
        return apiLabel;
    }

    public void setApiLabel(String apiLabel) {
        this.apiLabel = apiLabel;
    }

    public int getApiValue() {
        return apiValue;
    }

    public void setApiValue(int apiValue) {
        this.apiValue = apiValue;
    }

    public int compareTo(ApiVersion o) {
        if (o == null)
            return 1;
        if (o.getApiValue() == this.apiValue) {
            return 0;
        }
        if (o.getApiValue() < this.apiValue) {
            return 1;
        } else {
            return -1;
        }

    }
}
