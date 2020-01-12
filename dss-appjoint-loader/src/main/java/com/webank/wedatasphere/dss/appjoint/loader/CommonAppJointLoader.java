/*
 * Copyright 2019 WeBank
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.webank.wedatasphere.dss.appjoint.loader;

import com.webank.wedatasphere.dss.appjoint.AppJoint;
import com.webank.wedatasphere.dss.appjoint.clazzloader.AppJointClassLoader;
import com.webank.wedatasphere.dss.appjoint.utils.AppJointUtils;
import com.webank.wedatasphere.dss.appjoint.utils.ExceptionHelper;
import com.webank.wedatasphere.linkis.common.exception.ErrorException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * created by cooperyang on 2019/11/8
 * Description:
 */
public class CommonAppJointLoader implements AppJointLoader{


    private static final Logger logger = LoggerFactory.getLogger(CommonAppJointLoader.class);

    private final Map<String, AppJoint> appJoints = new HashMap<>();


    /**
     * 用来存放每一个appjoint所用到的classloader，这样的话，每一个appjoint的classloader都是不一样的
     */
    private final Map<String, ClassLoader> classLoaders = new HashMap<>();



    /**
     *
     * @param baseUrl appjoint代理的外部系统的url
     * @param appJointName appJoint的名字
     * @param params 参数用来进行init
     * @return
     * 命名规范必须是  ${DSS_HOME}/appjoints/${appjointName}/
     */
    @Override
    public AppJoint getAppJoint(String baseUrl, String appJointName, Map<String, Object> params) throws Exception{
        synchronized (appJoints){
            if (appJoints.containsKey(appJointName)){
                return appJoints.get(appJointName);
            }
        }
        if (params == null) {
            params = new HashMap<String, Object>();
        }
        //获取当前线程的类加载器
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader newClassLoader = null;
        synchronized (classLoaders){
            if (classLoaders.containsKey(appJointName)){
                newClassLoader = classLoaders.get(appJointName);
            }
        }
        //获取类路径的根路径，这里应该是/appcon/Install/DSSInstall/dss-sserver/lib
        URL classpathUrl = oldClassLoader.getResource("");
        logger.info("classpathUrl is {}", classpathUrl.getPath());
        if(null == classpathUrl){
            throw new ErrorException(70059, "classPathUrl is null");
        }
        //获取appjoint的路径  就是/appcon/Install/DSSInstall/dss-appjoint
        //感觉这里用相对路径不是很好
        String basePathUrlStr = classpathUrl.getPath() + ".." + File.separator + ".." + File.separator + AppJointLoader.APPJOINT_DIR_NAME;
        //获取到某个appjoin的lib报路径，比如：/appcon/Install/DSSInstall/dss-appjoint/qualitis/lib
        String libPathUrlStr =  basePathUrlStr + File.separator + appJointName +
                File.separator + AppJointLoader.LIB_NAME;
        //获取properties文件路径 ，比如：/appcon/Install/DSSInstall/dss-appjoint/qualitis/appjoint.properties
        String propertiesUrlStr = basePathUrlStr + File.separator + appJointName +
                File.separator + AppJointLoader.PROPERTIES_NAME;
        try{
            params.putAll(readFromProperties(propertiesUrlStr));
        }catch(IOException e){
            logger.warn("cannot get properties from {}", propertiesUrlStr, e);
        }
        URL finalURL = null;
        try {
            //file：///appcon/Install/DSSInstall/dss-sserver/lib
            finalURL = new URL(AppJointLoader.FILE_SCHEMA + libPathUrlStr + "/*");
        } catch (MalformedURLException e) {
            ExceptionHelper.dealErrorException(70061, libPathUrlStr + " url is wrong", e);
        }
        //获取lib包下所有的jar包的路径
        List<URL> jars = AppJointUtils.getJarsUrlsOfPath(libPathUrlStr);
        if (newClassLoader == null){
            //AppJointClassLoader extends URLClassLoader 基本上只是做个super的方法即可，然后将oldClassLoader 设置为parentclassloader
            //jars都放入类加载器中了，故不会冲突
            newClassLoader = new AppJointClassLoader(jars.toArray(new URL[100]) ,oldClassLoader);
        }
        synchronized (classLoaders){
            classLoaders.put(appJointName, newClassLoader);
        }
        //将当前线程的contextClassLoader设置为newClassLoader  那么当线程回收后，会出问题吗？？
        Thread.currentThread().setContextClassLoader(newClassLoader);
        //
        String fullClassName = AppJointUtils.getAppJointClassName(appJointName, libPathUrlStr, newClassLoader);
        Class clazz = null;
        try {
            //加载一下
            clazz = newClassLoader.loadClass(fullClassName);
        } catch (ClassNotFoundException e) {
            ExceptionHelper.dealErrorException(70062, fullClassName + " class not found ", e);
        }
        if (clazz == null){
            //返回类加载器
            Thread.currentThread().setContextClassLoader(oldClassLoader);
            return null;
        }else{
            //反射创建对象
            AppJoint retAppjoint = (AppJoint) clazz.newInstance();
            if (StringUtils.isEmpty(baseUrl) && params.get("baseUrl") != null){
                baseUrl = params.get("baseUrl").toString();
            }
            //调用init方法
            retAppjoint.init(baseUrl, params);
            //设置回旧的classCloader
            Thread.currentThread().setContextClassLoader(oldClassLoader);
            synchronized (appJoints){
                appJoints.put(appJointName, retAppjoint);
            }
            logger.info("appJointName is {},  retAppJoint is {}", appJointName, retAppjoint.getClass().toString());
            return retAppjoint;
        }
    }

    private Map<String, String> readFromProperties(String propertiesFile) throws IOException {
        Properties properties = new Properties();
        BufferedReader reader = new BufferedReader(new FileReader(propertiesFile));
        properties.load(reader);
        Map<String, String> map = new HashMap<String, String>((Map)properties);
        return map;
    }




}
