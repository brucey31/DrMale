package com.pannaman;


import junit.framework.TestCase;
import org.apache.commons.lang.ObjectUtils;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;
import junit.framework.Assert;
import org.apache.hadoop.io.Text;
import com.pannaman.DrMale;

import java.io.IOException;

import org.apache.pig.data.BinSedesTuple;
import org.joda.time.ReadableInstant;

public class DrMaleTest extends TestCase {

    private TupleFactory tupleFact = TupleFactory.getInstance();


    @Test
    public void testUDF() throws Exception {
        try { // NEW INSTANCE OF THE METHOD
            DrMale example = new DrMale();

            //SETTING WHAT YOU WANT IN AND OUT OF TEST
            String in = "miss";
            String expected = "Female";

            //MAKING THE INPUT STRING INTO A TUPLE THAT IS NEEDED FOR UDF INPUT
            Tuple input = DefaultTupleFactory.getInstance().newTuple(in);

            //RUN THIS INPUT THROUGH THE FUNCTION
            String output = example.exec(input);

            //IF INPUT AND OUTPUT MATCH WHAT YOU ARE EXPECTING, PASS THE TEST
            assertTrue(output.equals(expected));

        } catch (Exception e) {
            System.out.println("Dr-Male test has thrown and exception on the testUDF method " + e);
        }

    }

    @Test
    public void testUDFNullCheck() throws IOException {
        try {//CREATE NEW INSTANCE OF METHOD
            DrMale example = new DrMale();

            //PASS TEST IF NULL IS RECEIVED
            assertNull(example.exec(null));
        } catch (Exception e) {
            System.out.println("Dr-Male test has thrown and exception on the testUDFNullCheck method " + e);
        }
    }

    public void testUDFEmptyStringCheck() throws Exception {
        try {// NEW INSTANCE OF THE METHOD
            DrMale example = new DrMale();

            //SETTING WHAT YOU WANT IN AND OUT OF TEST
            String in = "";
            String expected = null;

            //MAKING THE INPUT STRING INTO A TUPLE THAT IS NEEDED FOR UDF INPUT
            Tuple input = DefaultTupleFactory.getInstance().newTuple(in);

            //RUN THIS INPUT THROUGH THE FUNCTION
            String output = example.exec(input);

            //IF INPUT AND OUTPUT MATCH WHAT YOU ARE EXPECTING, PASS THE TEST
            assertNull(output);
        } catch (Exception e) {
            System.out.println("Dr-Male test has thrown and exception on the testUDFEmptyStringCheck method " + e);
        }
    }

    public void testUDFSillyStringCheck() throws Exception {
        try {// NEW INSTANCE OF THE METHOD
            DrMale example = new DrMale();

            //SETTING WHAT YOU WANT IN AND OUT OF TEST
            String in = "ievbifubcibew";
            String expected = "Java Sucks Balls";

            //MAKING THE INPUT STRING INTO A TUPLE THAT IS NEEDED FOR UDF INPUT
            Tuple input = DefaultTupleFactory.getInstance().newTuple(in);

            //RUN THIS INPUT THROUGH THE FUNCTION
            String output = example.exec(input);

            //IF INPUT AND OUTPUT MATCH WHAT YOU ARE EXPECTING, PASS THE TEST
            assertTrue(output.equals(expected));

        } catch (Exception e) {
            System.out.println("Dr-Male test has thrown and exception on the testUDFEmptyStringCheck method " + e);
        }
    }

}
