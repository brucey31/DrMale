package com.pannaman;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.hadoop.hive.ql.exec.Description;


@Description(
        name = "DrMale",
        value = "You pass in a title from the MYCTM website and it returns the gender of the customer"
)
public class DrMale extends EvalFunc<String> {

    public String exec(Tuple input) {

        String Doc = input.toString().toLowerCase();

        if (Doc == "dr-male") {
            return new String("Male");
        } else if (Doc == "dr-female") {
            return new String("Female");
        } else if (Doc == "mr") {
            return new String("Male");
        } else if (Doc == "miss") {
            return new String("Female");
        } else if (Doc == "mrs") {
            return new String("Female");
        } else if (Doc == "ms") {
            return new String("Female");
        } else {
            return null;
        }


    }
}
