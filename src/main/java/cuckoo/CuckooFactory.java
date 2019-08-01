package cuckoo;

/**
 * 预警对象工厂
 * @Author huwenhu
 * @Date 2019/7/26 16:26
 **/
public class CuckooFactory {

    private CuckooFactory(){}

    private static CuckooFactory cuckooFactory = new CuckooFactory();

    public static CuckooFactory getInstance(){
        return cuckooFactory;
    }

    public CuckooInterface getCuckooService(String strCuckooType){

        if("email".equals(strCuckooType)){
            return new EmailCuckooService();
        }
        return null;
    }


}
