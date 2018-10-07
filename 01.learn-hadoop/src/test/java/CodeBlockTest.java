public class CodeBlockTest {
    static {
        System.out.println("statice");
    }

    public CodeBlockTest(){

    }

    {
        System.out.println("kkkkk");
    }

    public static void main(String[] args) {
        new CodeBlockTest();
    }
}
