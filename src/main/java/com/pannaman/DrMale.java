package com.pannaman;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.FuncSpec;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;


@Description(
        name = "DrMale",
        value = "You pass in a title from the MYCTM website and it returns the gender of the customer"
)
public class DrMale extends EvalFunc<String> {

    public String exec(Tuple input) throws IOException {


        if (input == null || input.size() == 0 || input.get(0) == null || input.get(0) == "") {
            return null;
        }


        try {

            String Doc = (String) input.get(0);
            String female = "Female";
            String male = "Male";

            if (Doc.toLowerCase().equals("dr-male") ) {
                return male;
            }

            if (Doc.toLowerCase().equals("dr-female")) {
                return female;
            }

            if (Doc.toLowerCase().equals( "mr")) {
                return male;
            }

            if (Doc.toLowerCase().equals("miss")) {
                return female;
            }

            if (Doc.toLowerCase().equals("mrs")) {
                return female;
            }

            if (Doc.toLowerCase().equals("ms")) {
                return female;
            }
            else {
                return null;
            }
        } catch (Exception e) {
            throw new IOException("DrMale has an error " + e);
        }

    }

    /**
     * This method gives a name to the column.
     *
     * @param input - schema of the input data
     * @return schema of the input data
     */
    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY));
    }

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));

        return funcList;
    }
}
