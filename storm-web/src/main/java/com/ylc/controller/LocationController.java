package com.ylc.controller;

import com.ylc.entity.Stat;
import com.ylc.jpa.StatJPA;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/location")
public class LocationController {

    @Autowired
    private StatJPA statJPA;

    @GetMapping("/index.html")
    public ModelAndView index(Model model) {
        System.out.println("=====view=====");
        ModelAndView mv = new ModelAndView();
        mv.setViewName("index");
        return mv;
    }

    @GetMapping("/data")
    public List<Map> data() {
        List<Map> all = statJPA.findData();
        return all;
    }
}
