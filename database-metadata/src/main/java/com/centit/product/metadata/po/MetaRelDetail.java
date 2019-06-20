package com.centit.product.metadata.po;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.hibernate.validator.constraints.Length;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotBlank;

/**
 * @author codefan
 * @author zouwy
 */
@Data
@Entity
@ApiModel(value = "关联明细")
@Table(name = "F_MD_REL_DETAIL")
public class MetaRelDetail implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 关联代码
     */
    @Id
    @Column(name = "RELATION_ID")
    @ApiModelProperty(hidden = true)
    private String relationId;

    /**
     * p字段代码
     */
    @Id
    @Column(name = "PARENT_COLUMN_CODE")
    @NotBlank(message = "字段不能为空")
    @ApiModelProperty(value = "父表列名")
    private String parentColumnName;

    /**
     * C字段代码
     */
    @Id
    @Column(name = "CHILD_COLUMN_CODE")
    @NotBlank(message = "字段不能为空")
    @ApiModelProperty(value = "子表列名")
    @Length(max = 32, message = "字段长度不能大于{max}")
    private String childColumnName;

    public MetaRelDetail copy(MetaRelDetail other) {
        this.parentColumnName = other.getParentColumnName();
        this.childColumnName = other.getChildColumnName();
        return this;
    }

    public MetaRelDetail copyNotNullProperty(MetaRelDetail other) {
        if (other.getParentColumnName() != null)
            this.parentColumnName = other.getParentColumnName();
        if (other.getRelationId() != null)
            this.relationId = other.getRelationId();
        if (other.getChildColumnName() != null)
            this.childColumnName = other.getChildColumnName();
        return this;
    }

    public MetaRelDetail clearProperties() {
        this.childColumnName = null;
        return this;
    }
}
