package voldemort.server.http.gui;

import voldemort.annotations.Experimental;
import voldemort.metrics.SensorRegistry;
import voldemort.server.VoldemortServer;
import voldemort.server.http.VoldemortServletContextListener;
import voldemort.utils.Utils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Proof of concept: exposing same statistics as through JMX, but via HTTP
 *
 */
@Experimental
public class MetricsServlet extends HttpServlet {

    private static final long serialVersionUID = 1;

    private VoldemortServer server;

    public MetricsServlet(VoldemortServer server) {

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

        Object sensorObject = registry.getSensor(sensorDomain, sensorType);
        if(sensorObject == null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        outputJSON(response, sensorObject);
    }

    protected void outputJSON(HttpServletResponse response, Object sensorObject) {
        // TODO: examine annotations, invoke methods and print the object
    }
}
