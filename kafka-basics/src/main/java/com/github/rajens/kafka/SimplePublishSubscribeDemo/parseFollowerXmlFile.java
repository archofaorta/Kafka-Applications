package com.github.rajens.kafka.SimplePublishSubscribeDemo;

import java.io.StringReader;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class parseFollowerXmlFile {

        public static void main(String[] argv) throws Exception {
            InputSource is = new InputSource();
            is.setCharacterStream(new StringReader(xmlRecords));

            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();

            Document doc = db.parse(is);
            StringBuilder sb = new StringBuilder();
            NodeList list = doc.getElementsByTagName("*");
            for (int i = 0; i < list.getLength(); i++) {
                Element element = (Element) list.item(i);
                sb.append("\\");
                sb.append(element.getNodeName());
            }

            System.out.println(sb.toString());
        }
    static String xmlRecords =
            "<data>" +
                    "  <employee>" +
                    "    <name>Rajen</name>"+
                    "    <title>Manager</title>" +
                    "  </employee>" +
                    "  <employee>" +
                    "    <name id='web'>java2s.com</name>"+
                    "    <title>Programmer</title>" +
                    "  </employee>" +
                    "</data>";


    }

