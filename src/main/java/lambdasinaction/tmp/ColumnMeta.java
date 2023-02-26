package lambdasinaction.tmp;


import java.io.Serializable;
import java.sql.Types;
import java.util.Map;
import java.util.Objects;

public final class ColumnMeta implements Serializable {
    /**
     * Uppercase column name
     */
    private String name;
    /**
     * SQL type from java.sql.Types
     */
    private int type;
    /**
     * Data source dependent type name
     */
    private String typeName;
    private Long length;
    private Long precision;
    private Integer scale;
    private Integer nullable;
    private String remarks;
    private String defaultValues;
    /**
     * 直接从数据库获取的列类型定义
     */
    private String dbSelfTypeDef;
    /**
     * 是否自增列(Y,N,或者空)
     */
    private String autoIncrement;
    private String lengthUnit;
    /**
     * 附加信息
     */
    private Map<String,Object> extraInfos;

    public ColumnMeta() {

    }
    public ColumnMeta(String name) {
        this.name = name;
    }

    public ColumnMeta(String name, int type, String typeName, Long length, Long precision, Integer scale, Integer nullable,
                      String remarks, String defaultValues) {
        this.name = name;
        this.type = type;
        this.typeName = typeName;
        this.length = length;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
        this.remarks = remarks;
        this.defaultValues = defaultValues;
    }

    public ColumnMeta copyThis(boolean ignoreDbSelfTypeDef) {
        ColumnMeta columnMeta = new ColumnMeta(name,type,typeName,length,precision,scale,nullable,remarks,defaultValues);
        if (!ignoreDbSelfTypeDef) {
            columnMeta.dbSelfTypeDef = dbSelfTypeDef;
            columnMeta.setExtraInfos(this.extraInfos);
        }
        return columnMeta;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public Long getLength() {
        return length;
    }

    public void setLength(Long length) {
        this.length = length;
    }

    public Long getPrecision() {
        return precision;
    }

    public void setPrecision(Long precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public Integer getNullable() {
        return nullable;
    }

    public void setNullable(Integer nullable) {
        this.nullable = nullable;
    }

    public boolean isNullable() {
        return !Objects.isNull(nullable) && (1 == nullable);
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public String getDefaultValues() {
        return defaultValues;
    }

    public void setDefaultValues(String defaultValues) {
        this.defaultValues = defaultValues;
    }

    public String getDbSelfTypeDef() {
        return dbSelfTypeDef;
    }

    public ColumnMeta setDbSelfTypeDef(String dbSelfTypeDef) {
        this.dbSelfTypeDef = dbSelfTypeDef;
        return this;
    }

    public String getAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(String autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public String getLengthUnit() {
        return lengthUnit;
    }

    public void setLengthUnit(String lengthUnit) {
        this.lengthUnit = lengthUnit;
    }

    public Map<String, Object> getExtraInfos() {
        return extraInfos;
    }

    public void setExtraInfos(Map<String, Object> extraInfos) {
        this.extraInfos = extraInfos;
    }

    private static boolean isCharType(int sqlType) {
        if (sqlType == Types.CHAR || sqlType == Types.VARCHAR || sqlType == Types.NCHAR ||
                sqlType == Types.NVARCHAR || sqlType == Types.LONGVARCHAR  || sqlType == Types.LONGNVARCHAR) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean biggerRangeThan(ColumnMeta one, ColumnMeta other) {
        if (one.getType() == other.getType()) {
            return one.getLength() > other.getLength();
        }

        if (isCharType(one.getType()) && isCharType(other.getType())) {
            return one.getLength() > other.getLength();
        }

        return false;
    }

    @Override
    public String toString() {
        return "ColumnMeta{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", typeName='" + typeName + '\'' +
                ", length=" + length +
                ", precision=" + precision +
                ", scale=" + scale +
                ", nullable=" + nullable +
                ", remarks='" + remarks + '\'' +
                ", defaultValues='" + defaultValues + '\'' +
                ", dbSelfTypeDef='" + dbSelfTypeDef + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnMeta)) {
            return false;
        }
        ColumnMeta that = (ColumnMeta) o;
        return type == that.type &&
                Objects.equals(name, that.name) &&
                Objects.equals(typeName, that.typeName) &&
                Objects.equals(length, that.length) &&
                Objects.equals(precision, that.precision) &&
                Objects.equals(scale, that.scale) &&
                Objects.equals(nullable, that.nullable) &&
                Objects.equals(remarks, that.remarks) &&
                Objects.equals(defaultValues, that.defaultValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, typeName, length, precision, scale, nullable, remarks, defaultValues);
    }
}
