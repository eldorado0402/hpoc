package com.metatron.util;

import java.io.IOException;
import java.util.Properties;

public class SQLConfiguration {

    private Properties props;

    public SQLConfiguration() throws IOException {
        this.props = new Properties();
        this.props.load(this.getClass().getClassLoader().getResourceAsStream("config.properties"));
    }

    public String get(String propertyName, String defaultName) {
        return this.props.getProperty(propertyName, defaultName);
    }

    public String get(String propertyName) {
        return this.props.getProperty(propertyName);
    }

    @Override
    public String toString() {
        return "SQLConfiguration{" +
                "props=" + props +
                '}';
    }
}
