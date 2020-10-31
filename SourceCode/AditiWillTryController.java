/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SourceCode;

import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Label;

/**
 * FXML Controller class
 *
 * @author ankitpagar
 */
public class AditiWillTryController implements Initializable {

    /**
     * Initializes the controller class.
     */
    
    @FXML
    public Label label;
    
    @FXML
    public void handleButtonAction(ActionEvent event) throws IOException{
        label.setText("Hello");
    }
    
    @Override
    public void initialize(URL url, ResourceBundle rb) {
        // TODO
    }    
    
}
