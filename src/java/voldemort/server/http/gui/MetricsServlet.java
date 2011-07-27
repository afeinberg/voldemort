package voldemort.server.http.gui;

import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.annotations.Experimental;
import voldemort.annotations.metrics.Attribute;
import voldemort.metrics.SensorRegistry;
import voldemort.server.VoldemortServer;
import voldemort.server.http.VoldemortServletContextListener;
import voldemort.utils.Utils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;

/**
 * Proof of concept: exposing same statistics as through JMX, but via HTTP
 *
 */
@Experimental
public class MetricsServlet extends HttpServlet {

    private static final long serialVersionUID = 1;

    private static final Logger logger = Logger.getLogger(MetricsServlet.class);

    private VoldemortServer server;

    public MetricsServlet(VoldemortServer server) {
        this.server = server;
    }

    @Override
    public void init() throws ServletException {
        super.init();
        this.server =  (VoldemortServer) Utils.notNull(getServletContext().getAttribute(VoldemortServletContextListener.SERVER_KEY));
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String sensorDomain = request.getParameter("domain");
        String sensorType = request.getParameter("type");
        SensorRegistry registry = server.getSensorRegistry();
        if(registry == null) {
            response.sendError(HttpServletResponse.SC_NO_CONTENT);
            return;
        }

        Object sensorObject = registry.getSensor(sensorDomain, sensorType);
        if(sensorObject == null) {
            if(logger.isInfoEnabled())
                logger.info("Not found domain=" + sensorDomain + ", type=" + sensorType);
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        outputJSON(response, sensorObject);
    }

    protected void outputJSON(HttpServletResponse response, Object sensorObject)
            throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        Class<?> cls = sensorObject.getClass();
        int i = 0;
        for(Method method: cls.getDeclaredMethods()) {
            if(method.isAnnotationPresent(Attribute.class)) {
                Attribute attribute = method.getAnnotation(Attribute.class);

                if(i++ > 0)
                    sb.append(",\n");

                sb.append("  \"");
                sb.append(attribute.name());
                sb.append("\": {");

                sb.append("\n    \"description\": \"");
                sb.append(attribute.description());
                sb.append("\", ");

                sb.append("\n    \"data_type\": \"");
                sb.append(attribute.dataType());
                sb.append("\", ");

                sb.append("\n    \"metric_type\": \"");
                sb.append(attribute.metricType());
                sb.append("\", ");

                sb.append("\n    \"value\": \"");
                try {
                    sb.append(method.invoke(sensorObject));
                } catch(Exception e) {
                    response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                    return;
                }
                sb.append("\"\n  }");
            }
        }
        sb.append("\n}\n");

        try {
            response.setContentType("text/plain");
            OutputStreamWriter writer = new OutputStreamWriter(response.getOutputStream());
            writer.write(sb.toString());
            writer.flush();
        } catch(Exception e) {
            throw new VoldemortException(e);
        }
    }
}
