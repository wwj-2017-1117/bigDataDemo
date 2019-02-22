package com.rickiyang.MultiInputData;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

/**
 * @Author yangyue
 * @Date Created in 下午5:30 2019/2/22
 * @Modified by:
 * @Description:
 **/
public class DataOrganUtility {

    public static String nestElements(DocumentBuilderFactory dbf, String post, List<String> comments) {
        try {
            DocumentBuilder bldr = dbf.newDocumentBuilder();
            Document doc = bldr.newDocument();
            Element postEl = getXmlElementFromString(dbf, post);
            Element toAddPostEl = doc.createElement("post");
            copyAttributesToElement(postEl.getAttributes(), toAddPostEl);
            for (String commentXml : comments) {
                Element commentEl = getXmlElementFromString(dbf, commentXml);
                Element toAddCommentEl = doc.createElement("comments");
                copyAttributesToElement(commentEl.getAttributes(),
                        toAddCommentEl);
                toAddPostEl.appendChild(toAddCommentEl);
            }
            doc.appendChild(toAddPostEl);
            return transformDocumentToString(doc);
        } catch (Exception e) {
            return null;
        }
    }

    public static Element getXmlElementFromString(DocumentBuilderFactory dbf, String xml) {
        try {
            DocumentBuilder bldr = dbf.newDocumentBuilder();
            return bldr.parse(new InputSource(new StringReader(xml)))
                    .getDocumentElement();
        } catch (Exception e) {
            return null;
        }
    }

    public static void copyAttributesToElement(NamedNodeMap attributes, Element element) {
        for (int i = 0; i < attributes.getLength(); ++i) {
            Attr toCopy = (Attr) attributes.item(i);
            element.setAttribute(toCopy.getName(), toCopy.getValue());
        }
    }

    public static String transformDocumentToString(Document doc) {
        try {
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc), new StreamResult(writer));
            return writer.getBuffer().toString().replaceAll("\n|\r", "");
        } catch (Exception e) {
            return null;
        }
    }
}