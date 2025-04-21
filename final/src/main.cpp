#include "controller.hpp"
#include "controller_round1.hpp"
#include "controller_round2.hpp"

Controller controller;

int main() {
    controller.init();
    controller.interact();
    return 0;
}