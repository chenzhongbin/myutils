${package};
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.wondersgroup.fgdj.constant.BusinessConstants;

@Entity
@Table(name = BusinessConstants.FGDJ_DB_NAME + "${tableName}")
public class ${className} {

<#list properties as property>
	private ${property.type} ${property.name};
</#list>

	@Id//注意调整
<#list methods as method>
	@Column(name="${method.columnName}")
	public ${method.type} ${method.get}() {
		return this.${method.name};
	}
	
	public void ${method.set}(${method.type} ${method.name}) {
		this.${method.name} = ${method.name};
	}
	
</#list>

}
