package com.examination.zhangfang.distributed.enums;

public enum BusinessEnum {
    USER(0, "用户"),
    TRADE(1, "交易");
    private int code;
    private String name;

    BusinessEnum(Integer code, String name) {
        this.code = code;
        this.name = name;
    }

    public static BusinessEnum getValue(Integer code) {
        if (code == null) {
            return null;
        }
        for (BusinessEnum businessEnum : BusinessEnum.values()) {
            if (businessEnum.getCode() == code) {
                return businessEnum;
            }
        }
        return null;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
