package util.ManagedStringBuilder;


/**
 * Somewhat identical class as a stringbuilder.
 * optimised for the current project. Only works with one single array.
 *
 * @author FlorianSauer
 */
public class ManagedStringBuilder {
    private char NULLCHAR;
    private int count;
    private char[] value;
    public ManagedStringBuilder(char[] initArray){
        this.count = 0;
        this.value = initArray;
    }
    public int length() {
        return this.count;
    }
    public int capacity() {
        return this.value.length;
    }
    public void empty(){
        for (int i = 0; i < this.count; i++){
            this.value[i] = this.NULLCHAR;
        }
        this.count = 0;
    }
    public ManagedStringBuilder append(String str){
        if (str == null)
            str = "null";
        int len = str.length();
        if (len + this.count >= this.value.length){
            throw new ArrayIndexOutOfBoundsException();
        }
        str.getChars(0, len, this.value, this.count);
        count += len;
//        System.out.println(this.toString());
        return this;
    }
    public ManagedStringBuilder append(int i) {
        if (i == Integer.MIN_VALUE) {
            append("-2147483648");
            return this;
        }
        return this.append(""+i);
    }
    @Override
    public String toString() {
        // Create a copy, don't share the array
        return new String(this.value, 0, this.count);
    }
}
