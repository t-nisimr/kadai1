#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
#  
# Copyright (c) 2013, Masanori Ishino.
# All rights reserved.
# 
# $Id: loadbalancer.rb 2013/10/28 20:41:00 m-ishino Exp $
#
require "rubygems"
require "pp"
require "counter"
require "router-utils"

class LoadBalancer < Controller
  periodic_timer_event :show_counter, 10
  START_SERVER = 200
  MAX_SERVER_COUNT = 254 - START_SERVER
  include RouterUtils

  def start
    @counter = Counter.new
    @fdb = {}
    @hard_timeout = 1
    @list_server = {}
    @mod_for_server = 0
    @server_count = 5
    @original_dst_server = "192.168.0.250"
  end

  def switch_ready( dpid )
    #get_server_list( dpid )
  end
  
  def switch_disconnected( dpid )
  end

  def packet_in( dpid, message )
    macsa = message.macsa
    macda = message.macda
    # fdb に message の macsa と in_port を学習させる
    @fdb [ macsa ] = message.in_port
    @counter.add macsa, 1, message.total_len
    # message の macda からポート番号を fdb から引く
    port = @fdb [ macda ]
    
    # For Debug
    if message.arp_request?
      #puts "arp_request from " + message.arp_spa.to_s + " to " + message.arp_tpa.to_s
      handle_arp_request( dpid, message )
    elsif message.arp_reply?
      #puts "arp_reply from " + message.arp_spa.to_s + " to " + message.arp_tpa.to_s
      handle_arp_reply( dpid, message )
    elsif message.rarp_request?
      #puts "rarp_request from " + message.rarp_spa.to_s + " to " + message.rarp_tpa.to_s
    elsif message.rarp_reply?
      #puts "rarp_reply from " + message.rarp_spa.to_s + " to " + message.rarp_tpa.to_s
    elsif message.ipv4?
      #puts "ipv4 from " + message.ipv4_saddr.to_s + " to " + message.ipv4_daddr.to_s
      handle_ipv4( dpid, message, port )
    else
      get_server_list( dpid )
      #puts "Other"
    end 
  end

  def flow_removed dpid, message
    @counter.add message.match.dl_src, message.packet_count, message.byte_count
  end


  private
  def get_server_list dpid
    for i in 0..MAX_SERVER_COUNT
      last_number = START_SERVER + i
      target_ip_addr = "192.168.0." + last_number.to_s
      arp_request_message = create_arp_request_from(Mac.new("00:00:00:00:00:00"), IPAddr.new(target_ip_addr), IPAddr.new("192.168.0.127"))
      send_packet_out(
        dpid,
        :data => arp_request_message,
        :actions => Trema::SendOutPort.new( OFPP_FLOOD )
      )
    end
  end
  def handle_ipv4 dpid, message, port
    last_number_daddr = message.ipv4_daddr.to_s.split(".")[3].to_i
    
    if last_number_daddr >= 128
      to_server = true
      next_server_select
    else
      to_server = false
    end

    if to_server
      if port
        action = [
        Trema::SetIpDstAddr.new(@next_server),
        Trema::SetEthDstAddr.new(@list_server[@next_server].to_s),
        Trema::SendOutPort.new(@fdb[@list_server[@next_server]])
        ]
        flow_mod( dpid, message, action )
        packet_out( dpid, message, action )
      else
        flood( dpid, message )
      end
    else
      if port
        action = [
        Trema::SetIpSrcAddr.new(@original_dst_server),
        Trema::SetEthSrcAddr.new(@list_server[@original_dst_server].to_s),
        Trema::SendOutPort.new( port )
        ]
        flow_mod( dpid, message, action )
        packet_out( dpid, message, action )
      else
        flood( dpid, message )
      end
    end
  end

  def handle_arp_reply dpid, message
    list_check = message.arp_spa.to_s
    if ((list_check.split(".")[3].to_i >= START_SERVER) && (!(@list_server.has_key?(list_check))))
      @list_server[list_check] = message.arp_sha
    else
      flood( dpid, message )
    end
  end

  def handle_arp_request dpid, message
    flood( dpid, message )
  end

  def show_counter
    puts Time.now
    @counter.each_pair do | mac, counter |
      puts "#{ mac } #{ counter[ :packet_count ] } packets (#{ counter[ :byte_count ] } bytes)"
    end
  end

  def flow_mod( dpid, message, action )
    send_flow_mod_add(
      dpid,
      :hard_timeout => @hard_timeout,
      :match => Match.from( message ),
      :actions => action
    )
  end
  
  def packet_out( dpid, message, action )
    send_packet_out(
      dpid,
      :packet_in => message,
      :actions => action
    )
  end
  
  def flood( dpid, message )
    packet_out(
      dpid,
      message,
      Trema::SendOutPort.new( OFPP_FLOOD )
    )
  end
  
  def next_server_select
    @next_server = @list_server.keys[ @mod_for_server % @server_count ]
    @mod_for_server += 1
  end
end

### Local variables:
### mode: Ruby
### coding: utf-8
### indent-tabs-mode: nil
### End:
