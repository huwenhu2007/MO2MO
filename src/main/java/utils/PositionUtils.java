package utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import oplog.MongoConfig;
import oplog.entity.OplogEvent;
import oplog.entity.OplogPosition;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author huwenhu
 * @Date 2019/7/12 17:12
 **/
public class PositionUtils {

    private static Logger logger = Logger.getLogger(PositionUtils.class);

    /**
     * 位置信息文件缓存
     */
    private static HashMap<String, File> mapFile = new HashMap<>();
    private static HashMap<String, File> mapFileBak = new HashMap<>();

    /**
     * 位置信息持久化
     * @param strOplogPosition
     * @param nTime
     * @param nIncrement
     * @throws IOException
     */
    public static  void  writeEndPosition(String strOplogPosition, int nTime, int nIncrement) throws IOException {
        File file=null;
        File fileBak=null;
        if(!mapFile.containsKey(strOplogPosition)){
            file=new File(strOplogPosition);
            if(!file.exists()){
                file.createNewFile();
            }
            fileBak=new File(new StringBuilder(strOplogPosition).append("_bak").toString());
            if(!fileBak.exists()){
                fileBak.createNewFile();
            }
            mapFile.put(strOplogPosition, file);
            mapFileBak.put(strOplogPosition, fileBak);
        }

        file = mapFile.get(strOplogPosition);
        fileBak = mapFileBak.get(strOplogPosition);
        // 备份当前位置信息,防止进程停止时文件信息为空的问题
        FileUtils.copyFile(file, fileBak);
        // 保存位置信息到本地
        StringBuilder strContext=new StringBuilder();
        strContext.append(nTime).append("|").append(nIncrement);
        Utilitys.write(file, strContext.toString());

    }

    public static OplogPosition findLoaclOplogPositionList(List<String> list, String strIP, int nPort, String strWorkId) throws IOException {
        OplogPosition position=new OplogPosition();
        position.setStrIP(strIP);
        position.setnPort(nPort);
        position.setStrWorkId(strWorkId);

        int nListSize = list.size();
        OplogPosition[] arrOplogPosition = new OplogPosition[nListSize];
        int nSuccessSize = 0;
        for(int i = 0; i < nListSize;i++){
            String strPath = list.get(i);
            File file=new File(strPath);
            if(!file.exists()){
                logger.info(String.format("%s 文件不存在",strPath));
                continue ;
            }
            String content=FileUtils.readFileToString(file);
            if(StringUtils.isBlank(content)){
                logger.info(String.format("%s 文件内容为空",strPath));
                // 获取备份信息
                File fileBak=new File(new StringBuilder(strPath).append("_bak").toString());
                if(fileBak.exists()){
                    content=FileUtils.readFileToString(fileBak);
                } else {
                    continue ;
                }
            }
            content=content.replace("\r\n","");
            String[] positionInfo=content.split("\\|");
            if(positionInfo.length!=2){
                logger.info(String.format("%s 文件内容格式错误 %s", strPath, content));
                continue ;
            }

            try {
                int nTime = Integer.parseInt(positionInfo[0]);
                int nIncrement = Integer.parseInt(positionInfo[1]);
                OplogPosition p = new OplogPosition();
                p.setnTime(nTime);
                p.setnIncrement(nIncrement);
                p.setStrSign(strPath.substring(strPath.lastIndexOf(File.separator) + 1));
                arrOplogPosition[nSuccessSize ++] = p;
            } catch (Exception e) {
                logger.info(String.format("%s 文件内容格式错误 %s", strPath, content));
                continue ;
            }
        }

        if(arrOplogPosition[0] == null){
            logger.info(String.format("%s%d%s位置信息不存在", strIP, nPort, strWorkId));
            return null;
        }

        // 获取最早时间的位置信息
        OplogPosition p = arrOplogPosition[0];
        for (int i = 1; i < nSuccessSize; i++) {
            OplogPosition p1 = arrOplogPosition[i];
            int nTime1 = p1.getnTime();
            int nIncrement1 = p1.getnIncrement();

            int nTime = p.getnTime();
            int nIncrement = p.getnIncrement();

            if (nTime > nTime1) {
                p = p1;
            } else if (nTime == nTime1 && nIncrement >= nIncrement1) {
                p = p1;
            }
        }

        position.setStrSign(p.getStrSign());
        position.setnTime(p.getnTime());
        position.setnIncrement(p.getnIncrement());
        return position;
    }

    public static void main(String[] args) {
        Date time = new Date();

        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(time.getTime());

        System.out.println(sdf.format(new Long(1510049394) * 1000));

        System.out.println("d://jjaj//jjdh//sda//www22".substring("d://jjaj//jjdh//sda//www22".lastIndexOf("/") + 1));
    }

    public static OplogPosition findDefaultOplogPosition(MongoConfig mongoConfig) throws IOException {
        OplogPosition oplogPosition=new OplogPosition();
        oplogPosition.setStrIP(mongoConfig.getStrIP());
        oplogPosition.setnPort(mongoConfig.getnPort());
        oplogPosition.setStrWorkId(mongoConfig.getStrWorkerId());

        long lTime = System.currentTimeMillis() / 1000L;
        try {
            oplogPosition.setnTime((int)lTime);
        } catch (Exception e) {
            logger.warn("findDefaultOplogPosition strOplogPosition  lTime to int "+lTime+" ",e);
            return null;
        }
        oplogPosition.setnIncrement(1);
        return	oplogPosition;
    }

    /**
     * 位置信息文件保存目录缓存
     */
    private static HashSet<String> setDir = new HashSet<>();

    /**
     * 创建位置信息目录
     * @param strWorkSign
     * @param strEndLogPosPath
     * @param strIp
     * @param nPort
     * @param strWorkId
     */
    public static void buildEndPositionFileDir(String strWorkSign, String strEndLogPosPath, String strIp, int nPort, String strWorkId){
        if(setDir.contains(strWorkSign)){
            return ;
        }

        StringBuilder strFileName=new StringBuilder();
        strFileName.append(strEndLogPosPath).append(strIp)
                .append(nPort).append(strWorkId);
        String strFileDir = strFileName.toString();
        File dir = new File(strFileDir);
        if(!dir.exists()){
            dir.mkdirs();
        }
        setDir.add(strWorkSign);
    }

    public static String buildEndPositionFileName(String strEndLogPosPath, String strIp, int nPort, String strWorkId, String strSign){
        StringBuilder strFileName=new StringBuilder();
        strFileName.append(strEndLogPosPath).append(strIp)
                .append(nPort).append(strWorkId).append(File.separator).append(strSign);
        return strFileName.toString();
    }

    /**
     * 获取多目录信息
     * @param mongoConfig
     * @return
     */
    public static List<String> buildEnistdPositionFileNameList(MongoConfig mongoConfig){
        List<String> list = new ArrayList<>();

        JSONArray dmlTargetJSONArray = mongoConfig.getStrDMLTargetJSONArray();
        int nLength = dmlTargetJSONArray.size();
        for(int i = 0;i < nLength;i++) {
            JSONObject jsonObject = dmlTargetJSONArray.getJSONObject(i);
            StringBuilder strFileName = new StringBuilder();
            strFileName.append(mongoConfig.getStrEndLogPosPath()).append(mongoConfig.getStrIP())
                    .append(mongoConfig.getnPort()).append(mongoConfig.getStrWorkerId()).append(File.separator).append(jsonObject.getString("strSign"));
            list.add(strFileName.toString());
        }
        return list;
    }

    /**
     * 获取可用位置信息
     * @param mongoConfig
     * @return
     * @throws IOException
     */
    public static OplogPosition findBinlogPositionList(MongoConfig mongoConfig) throws IOException {
        OplogPosition  position=null;

        if(1 == mongoConfig.getnPositionEnable()){
            position=new OplogPosition(mongoConfig.getStrIP(), mongoConfig.getnPort(), mongoConfig.getStrWorkerId(), "config", mongoConfig.getnTime(), mongoConfig.getnIncrement());
            return position;
        }

        List<String> list = buildEnistdPositionFileNameList(mongoConfig);
        position = findLoaclOplogPositionList(list,mongoConfig.getStrIP(),mongoConfig.getnPort(),mongoConfig.getStrWorkerId());
        if(position==null){
            logger.info(String.format("%s使用当前时间作为位置信息", mongoConfig.toString()));
            position = PositionUtils.findDefaultOplogPosition(mongoConfig);
        }
        return position;
    }

}
