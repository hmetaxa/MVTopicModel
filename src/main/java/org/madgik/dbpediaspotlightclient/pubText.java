/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.madgik.dbpediaspotlightclient;

/**
 *
 * @author omiros
 */
 public class pubText {

        public pubText(String pubId, String text) {
            this.pubId = pubId;
            this.text = text;

        }

        private String pubId;
        private String text;

        public String getPubId() {
            return pubId;
        }

        public void setPubId(String PubId) {
            this.text = PubId;
        }

        public String getText() {
            return text;
        }

        public void setText(String Text) {
            this.text = Text;
        }

    }