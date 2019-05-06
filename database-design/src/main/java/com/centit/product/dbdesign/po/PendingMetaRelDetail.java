package com.centit.product.dbdesign.po;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.constraints.NotBlank;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;


/**
 * create by scaffold 2016-06-01
 * <p>
 * <p>
 * 未落实表关联细节表null
 */
@ApiModel
@Data
@Entity
@Table(name = "F_PENDING_META_REL_DETIAL")
public class PendingMetaRelDetail implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    @ApiModelProperty(value = "关联代码", hidden = true)
    @Id
    @Column(name = "RELATION_ID")
    private String relationId;

    @ApiModelProperty(value = "p字段代码")
    @Id
    @Column(name = "PARENT_COLUMN_NAME")
    @NotBlank(message = "字段不能为空")
    private String parentColumnName;

    @ApiModelProperty(value = "C字段代码")
    @Column(name = "CHILD_COLUMN_NAME")
    @NotBlank(message = "字段不能为空")
    @Length(max = 32, message = "字段长度不能大于{max}")
    private String childColumnName;

    // Constructors

    /**
     * default constructor
     */
    public PendingMetaRelDetail() {
    }

    /**
     * minimal constructor
     */
    public PendingMetaRelDetail(String relationId, String parentColumnName) {
        this.relationId = relationId;
        this.parentColumnName = parentColumnName;
    }

    public PendingMetaRelDetail(String relationId, String parentColumnName, String childColumnName) {
        this.relationId = relationId;
        this.parentColumnName = parentColumnName;
        this.childColumnName = childColumnName;
    }


    public PendingMetaRelDetail copy(PendingMetaRelDetail other) {

        this.setRelationId(other.getRelationId());
        this.setParentColumnName(other.getParentColumnName());

        this.childColumnName = other.getChildColumnName();

        return this;
    }

    public PendingMetaRelDetail copyNotNullProperty(PendingMetaRelDetail other) {

        if (other.getRelationId() != null)
            this.setRelationId(other.getRelationId());
        if (other.getParentColumnName() != null)
            this.setParentColumnName(other.getParentColumnName());

        if (other.getChildColumnName() != null)
            this.childColumnName = other.getChildColumnName();

        return this;
    }

    public PendingMetaRelDetail clearProperties() {

        this.childColumnName = null;

        return this;
    }
}
