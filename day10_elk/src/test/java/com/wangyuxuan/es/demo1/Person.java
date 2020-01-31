package com.wangyuxuan.es.demo1;

/**
 * @author wangyuxuan
 * @date 2020/1/31 5:09 下午
 * @description 定义Person对象
 */
public class Person {
    private Integer id;
    private String name;
    private Integer age;
    private Integer sex;
    private String address;
    private String phone;
    private String email;
    private String say;

    public Person() {
    }

    public Person(Integer id, String name, Integer age, Integer sex, String address, String phone, String email, String say) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.address = address;
        this.phone = phone;
        this.email = email;
        this.say = say;
    }

    public String getSay() {
        return say;
    }

    public void setSay(String say) {
        this.say = say;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Integer getSex() {
        return sex;
    }

    public void setSex(Integer sex) {
        this.sex = sex;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}
