package com.pannaman;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.hadoop.hive.ql.exec.Description;

import java.io.IOException;


@Description(
        name = "DrMale",
        value = "You pass in a title from the MYCTM website and it returns the gender of the customer"
)
public class DrMale extends EvalFunc<String> {

    public String exec(Tuple input) throws IOException, NullPointerException {


        if (input.size() == 0 || input.get(0) == "" || input.get(0) == null || input == null || input.get(0).toString() == null) {
            return null;
        }


        try {

            String Doc = input.get(0).toString();
            String female = "Female";
            String male = "Male";

            if (Doc.toLowerCase().equals("dr-male")) {
                return male;
            }

            if (Doc.toLowerCase().equals("dr-female")) {
                return female;
            }

            if (Doc.toLowerCase().equals("mr")) {
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
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new IOException("DrMale has an IOerror " + e);
        } catch (NullPointerException e) {
            throw new NullPointerException("DrMale has a null error " + e);
        }

    }

    /*
     * This method gives a name to the column.
     *
     * @param input - schema of the input data
     * @return schema of the input data


    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY, DataType.NULL));
    }
*/
    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));

        return funcList;
    }*/
}
